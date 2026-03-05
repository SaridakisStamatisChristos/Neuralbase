"""DQN training for the NeuralBase join-order optimizer.

Architecture
------------
State:  float32[1, 72]  — 8×8 matrix + 8 cardinality features
  off-diagonals A[i,j]: selectivity feature between global tables i and j
      = clamp(-log10(sel) / LOG_NORM, 0, 1)  — constant across episodes
  diagonal A[i,i]:      join-position feature for table i
      = (pos+1) / ACTION_DIM  (0.0 if not yet joined)
  tail [64:72]:         log-cardinality of present tables (0.0 for absent)

Action: int in [0, 7]  — GLOBAL TPC-H table index, stable across all episodes
Reward: -hash_join_cost / COST_SCALE + fk_bonus per join step

Network: Linear(72→256) → ReLU → Linear(256→256) → ReLU → Linear(256→8)

Cost model (matches cost_model.rs)
-----------------------------------
  hash join cost  = build_side * 2 + probe_side   (build = smaller side)
  selectivity     = FK table if pair known, else DEFAULT_SELECTIVITY (0.01)
  output rows     = left * right * selectivity  (capped at MAX_INTERMEDIATE_ROWS)
  rewards scaled  by COST_SCALE = 1e6 so rewards are O(1)
  FK join bonus   = +0.1 when joining on a known foreign-key pair

Training
--------
  * epsilon-greedy exploration (epsilon decays 1.0 -> 0.05 over EPSILON_DECAY steps)
  * Experience replay (buffer = REPLAY_SIZE transitions)
  * Target network hard-updated every TARGET_UPDATE steps
  * Bootstrap target applies the SAME action mask as selection (no optimistic bias)
  * Adam optimizer, lr = LR

Output
------
  Saves the trained model to ../model/neuralbase_optimizer.onnx on completion.

Usage
-----
  pip install -r requirements.txt
  python train.py [--steps 200000] [--output ../model/neuralbase_optimizer.onnx]
"""

import argparse
import math
import os
import random
from collections import deque
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim

# ── Hyper-parameters ──────────────────────────────────────────────────────────

STATE_DIM = 72       # 8×8 matrix + 8 cardinality features
ACTION_DIM = 8       # global TPC-H table count (stable action space)
LOG_NORM = 6.0       # log10 normalisation denominator
COST_SCALE = 1_000_000.0  # normalize costs so rewards are O(1)
# Cap intermediate row count to prevent exponential blowup for bad orderings.
MAX_INTERMEDIATE_ROWS = 5_000_000

LR = 1e-3
GAMMA = 0.99
BATCH_SIZE = 64
REPLAY_SIZE = 50_000     # larger buffer → more diverse experience replay
TARGET_UPDATE = 100
EPSILON_START = 1.0
EPSILON_END = 0.05
# EPSILON_DECAY controls how fast exploration decays.
# Rule: set to ~30% of total_steps so epsilon ≈ 0.4 at the midpoint.
# Default (200k steps): 100_000 → ε≈0.40 at 100k, ε≈0.18 at 200k.
# Override per run via the argparse --epsilon-decay flag.
EPSILON_DECAY = 100_000

# ── TPC-H table sizes at SF 0.1 ───────────────────────────────────────────────

TPCH_TABLES = {
    "lineitem": 600_122,
    "orders":   150_000,
    "customer":  15_000,
    "supplier":   1_000,
    "part":      20_000,
    "partsupp":  80_000,
    "nation":        25,
    "region":         5,
}

TPCH_TABLE_LIST = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
]

# ── TPC-H foreign-key join selectivity ────────────────────────────────────────
# Derived from the bench cost model's join_selectivity():
#   sel = 1 / max(NDV_left, NDV_right)
# where NDV defaults to row_count when column stats are absent (which is the
# case for tpch_stats() — no ColumnStats are populated).
# This MUST match the values computed by cost_model.rs / join_graph.rs exactly
# so the DQN is trained on the same cost function the benchmark evaluates.
#
#   Table rows (SF 0.1):
#   lineitem=600122  orders=150000  customer=15000  supplier=1000
#   part=20000       partsupp=80000  nation=25       region=5

SELECTIVITY: Dict[Tuple[str, str], float] = {
    # FK pair              bench sel = 1/max(rows_a, rows_b)
    ("orders",   "lineitem"):  1.0 / 600_122,  # 1/max(150k, 600122) ≈ 1.67e-6
    ("customer", "orders"):    1.0 / 150_000,  # 1/max(15k,  150k)   ≈ 6.67e-6
    ("nation",   "supplier"):  1.0 / 1_000,    # 1/max(25,   1k)     = 1e-3
    ("nation",   "customer"):  1.0 / 15_000,   # 1/max(25,   15k)    ≈ 6.67e-5
    ("region",   "nation"):    1.0 / 25,       # 1/max(5,    25)     = 0.04
    ("part",     "partsupp"):  1.0 / 80_000,   # 1/max(20k,  80k)    = 1.25e-5
    ("supplier", "partsupp"):  1.0 / 80_000,   # 1/max(1k,   80k)    = 1.25e-5
    # Additional FK edges that appear in TPC-H queries (were missing → fell
    # back to DEFAULT=0.01, a 6000× error vs bench's computed value):
    ("lineitem", "supplier"):  1.0 / 600_122,  # 1/max(600122, 1k)   ≈ 1.67e-6
    ("part",     "lineitem"):  1.0 / 600_122,  # 1/max(20k,  600122) ≈ 1.67e-6
    ("lineitem", "partsupp"):  1.0 / 600_122,  # 1/max(600122, 80k)  ≈ 1.67e-6
}

DEFAULT_SELECTIVITY = 0.01


def _lookup_selectivity(a: str, b: str) -> float:
    """Return FK selectivity for the (a, b) pair, checking both orientations."""
    return SELECTIVITY.get((a, b), SELECTIVITY.get((b, a), DEFAULT_SELECTIVITY))


# ── Precomputed selectivity feature matrix (constant across all episodes) ─────
# _SEL_FEAT[i, j] = clamp(-log10(sel(i,j)) / LOG_NORM, 0, 1)  for i != j
# _SEL_FEAT[i, i] = 0.0  — diagonal is reserved for the join-position feature

_SEL_FEAT = np.zeros((ACTION_DIM, ACTION_DIM), dtype=np.float32)
for _i in range(ACTION_DIM):
    for _j in range(ACTION_DIM):
        if _i != _j:
            _s = _lookup_selectivity(TPCH_TABLE_LIST[_i], TPCH_TABLE_LIST[_j])
            _SEL_FEAT[_i, _j] = min(-math.log10(_s) / LOG_NORM, 1.0)

_SEL_FEAT_FLAT: np.ndarray = _SEL_FEAT.flatten()  # cached flat copy

# Flat indices of the 8×8 diagonal: [0, 9, 18, 27, 36, 45, 54, 63]
DIAG_IDX: List[int] = [i * ACTION_DIM + i for i in range(ACTION_DIM)]
DIAG_IDX_T = torch.tensor(DIAG_IDX, dtype=torch.long)


# ── DQN network ───────────────────────────────────────────────────────────────

class DQN(nn.Module):
    """3-layer MLP: 72 → 256 → 256 → 8."""

    def __init__(self) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(STATE_DIM, 256),
            nn.ReLU(),
            nn.Linear(256, 256),
            nn.ReLU(),
            nn.Linear(256, ACTION_DIM),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


# ── Batch action masking ───────────────────────────────────────────────────────

def batch_action_mask(states: torch.Tensor) -> torch.Tensor:
    """Return a [B, 8] mask (0.0 = valid, -inf = invalid) derived from states.

    A global table index is valid iff:
      - present in the query  (tail cardinality feature [64+i] > 0)
      - not yet joined        (diagonal A[i,i] == 0)
    """
    present = (states[:, ACTION_DIM * ACTION_DIM:ACTION_DIM * ACTION_DIM + ACTION_DIM] > 0.0)
    joined  = (states[:, DIAG_IDX_T.to(states.device)] > 0.0)
    valid   = present & (~joined)
    mask    = torch.full((states.size(0), ACTION_DIM), float("-inf"), device=states.device)
    mask[valid] = 0.0
    return mask


# ── Environment ───────────────────────────────────────────────────────────────

class JoinOrderEnv:
    """Left-deep join construction with globally-stable action semantics.

    Actions are ALWAYS global TPC-H table indices (0..7).
    The episode is defined by present_mask[i]=1 for tables in this query.

    State layout (72-dim, matches ONNX input):
      [0:64]  flat 8×8 matrix
              - off-diagonal [i,j]: constant selectivity feature
              - diagonal     [i,i]: join position / ACTION_DIM  (0 if not joined)
      [64:72] log-cardinality of present tables (0 for absent)
    """

    def __init__(self, present_mask: List[int]) -> None:
        self.present = present_mask          # length ACTION_DIM, values 0/1
        self.n = sum(present_mask)           # tables in this query
        self.reset()

    def reset(self) -> np.ndarray:
        self.joined: List[int] = []   # global indices in join order
        self.running_rows = 0
        return self._state()

    def step(self, action: int) -> Tuple[np.ndarray, float, bool]:
        """Add global-table index `action` to the running left-deep plan."""
        if not self.present[action] or action in self.joined:
            # Invalid: penalty consistent with reward scale; episode ends
            return self._state(), -10.0, True

        table_name = TPCH_TABLE_LIST[action]

        if not self.joined:
            self.running_rows = TPCH_TABLES[table_name]
            reward = 0.0
        else:
            right_rows = TPCH_TABLES[table_name]

            # Most selective FK between the new table and any already-joined table
            sel = min(
                _lookup_selectivity(TPCH_TABLE_LIST[j], table_name)
                for j in self.joined
            )

            # Hash join: smaller = build side, larger = probe side
            build = min(self.running_rows, right_rows)
            probe = max(self.running_rows, right_rows)
            cost  = float(build * 2 + probe)

            # Compute uncapped output cardinality so overflow is detectable.
            uncapped_rows = max(1, int(self.running_rows * right_rows * sel))
            overflow = 1.0 if uncapped_rows >= MAX_INTERMEDIATE_ROWS else 0.0
            self.running_rows = min(MAX_INTERMEDIATE_ROWS, uncapped_rows)

            # FK bonus: joining on a known foreign-key relationship
            fk_bonus = 0.0
            for j in self.joined:
                pair = (TPCH_TABLE_LIST[j], table_name)
                rev  = (table_name, TPCH_TABLE_LIST[j])
                if pair in SELECTIVITY or rev in SELECTIVITY:
                    fk_bonus = 0.1
                    break

            # Cardinality penalty: makes the reward immediately sensitive to
            # selectivity rather than relying on delayed credit assignment.
            #   card_pen  ∈ [0, 1]  — log10(running_rows) / LOG_NORM
            #   overflow  ∈ {0, 1}  — hard hit when the cap fires; without this
            #                         the cap collapses "very bad" and "worst"
            #                         plans into the same reward, killing the
            #                         gradient between them.
            card_pen = math.log10(self.running_rows) / LOG_NORM
            reward = -cost / COST_SCALE - 0.05 * card_pen - 0.5 * overflow + fk_bonus

        self.joined.append(action)
        done = len(self.joined) == self.n
        return self._state(), reward, done

    # ── State encoding ───────────────────────────────────────────────────────

    def _state(self) -> np.ndarray:
        state = np.zeros(STATE_DIM, dtype=np.float32)

        # Off-diagonal: constant selectivity graph
        state[:ACTION_DIM * ACTION_DIM] = _SEL_FEAT_FLAT

        # Diagonal: join-position feature  (pos+1) / ACTION_DIM
        for pos, t in enumerate(self.joined):
            state[t * ACTION_DIM + t] = (pos + 1) / ACTION_DIM

        # Tail [64:72]: log-cardinality for present tables, 0 for absent
        for i in range(ACTION_DIM):
            if self.present[i]:
                rows = TPCH_TABLES[TPCH_TABLE_LIST[i]]
                state[64 + i] = min(math.log10(max(rows, 1)) / LOG_NORM, 1.0)

        return state


# ── Replay buffer ─────────────────────────────────────────────────────────────

Transition = Tuple[np.ndarray, int, float, np.ndarray, bool]


class ReplayBuffer:
    def __init__(self, capacity: int) -> None:
        self.buffer: deque[Transition] = deque(maxlen=capacity)

    def push(self, *args: object) -> None:
        self.buffer.append(args)  # type: ignore[arg-type]

    def sample(self, batch_size: int) -> List[Transition]:
        return random.sample(self.buffer, batch_size)  # type: ignore[return-value]

    def __len__(self) -> int:
        return len(self.buffer)


# ── Training loop ─────────────────────────────────────────────────────────────

def train(total_steps: int, output_path: str, epsilon_decay: int = EPSILON_DECAY) -> DQN:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Training on: {device}  epsilon_decay={epsilon_decay}  replay_size={REPLAY_SIZE}")
    policy_net = DQN().to(device)
    target_net = DQN().to(device)
    target_net.load_state_dict(policy_net.state_dict())
    target_net.eval()

    optimizer = optim.Adam(policy_net.parameters(), lr=LR)
    replay = ReplayBuffer(REPLAY_SIZE)

    step = 0
    episodes = 0
    # Use a rolling window for reward reporting so we see RECENT policy quality,
    # not a cumulative average dominated by early bad episodes.
    recent_rewards: deque = deque(maxlen=500)

    # Global index pool; actions are always from [0, ACTION_DIM)
    all_global = list(range(ACTION_DIM))

    while step < total_steps:
        # Sample a random subset of 2–8 tables for this episode.
        # present_mask[i] = 1 iff global table i participates.
        n_tables = random.randint(2, ACTION_DIM)
        chosen = random.sample(all_global, n_tables)
        present_mask = [1 if i in chosen else 0 for i in range(ACTION_DIM)]
        env = JoinOrderEnv(present_mask)

        state = env.reset()
        done = False
        episode_reward = 0.0

        while not done:
            epsilon = EPSILON_END + (EPSILON_START - EPSILON_END) * math.exp(
                -step / epsilon_decay
            )

            # Valid actions: globally-indexed tables that are present and unjoinned
            remaining = [i for i in all_global if present_mask[i] and i not in env.joined]
            if not remaining:
                break

            if random.random() < epsilon:
                action = random.choice(remaining)
            else:
                with torch.no_grad():
                    state_t = torch.FloatTensor(state).unsqueeze(0).to(device)
                    q = policy_net(state_t)
                    mask = torch.full((1, ACTION_DIM), float("-inf"), device=device)
                    for idx in remaining:
                        mask[0, idx] = 0.0
                    q = q + mask
                    action = int(q.argmax(dim=1).item())

            next_state, reward, done = env.step(action)
            episode_reward += reward
            replay.push(state, action, reward, next_state, done)
            state = next_state
            step += 1

            # Training step
            if len(replay) >= BATCH_SIZE:
                transitions = replay.sample(BATCH_SIZE)
                states_b  = torch.FloatTensor(np.stack([t[0] for t in transitions])).to(device)
                actions_b = torch.LongTensor([t[1] for t in transitions]).to(device)
                rewards_b = torch.FloatTensor([t[2] for t in transitions]).to(device)
                next_b    = torch.FloatTensor(np.stack([t[3] for t in transitions])).to(device)
                done_b    = torch.BoolTensor([t[4] for t in transitions]).to(device)

                current_q = policy_net(states_b).gather(1, actions_b.unsqueeze(1)).squeeze(1)
                with torch.no_grad():
                    # Fix B: mask invalid actions in the bootstrap target so the
                    # target network cannot pick up optimistic Q-values for
                    # actions that are impossible in the next state.
                    mask_next = batch_action_mask(next_b)
                    next_q = (target_net(next_b) + mask_next).max(1).values
                    next_q[done_b] = 0.0
                    target_q = rewards_b + GAMMA * next_q

                loss = nn.functional.smooth_l1_loss(current_q, target_q)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            if step % TARGET_UPDATE == 0:
                target_net.load_state_dict(policy_net.state_dict())

        recent_rewards.append(episode_reward)
        episodes += 1
        if episodes % 200 == 0:
            recent_avg = sum(recent_rewards) / len(recent_rewards)
            eps_now = EPSILON_END + (EPSILON_START - EPSILON_END) * math.exp(-step / epsilon_decay)
            print(f"  step {step:6d}  ep {episodes:5d}  recent_avg {recent_avg:+.1f}  eps {eps_now:.3f}")

    print(f"Training complete: {step} steps, {episodes} episodes")
    _export_onnx(policy_net, output_path)
    return policy_net


def _export_onnx(model: DQN, path: str) -> None:
    model.cpu()
    model.eval()
    dummy = torch.zeros(1, STATE_DIM)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    torch.onnx.export(
        model,
        dummy,
        path,
        input_names=["state"],
        output_names=["q_values"],
        opset_version=13,
        dynamic_axes=None,
    )
    print(f"Exported ONNX model to {path}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train NeuralBase DQN optimizer")
    parser.add_argument("--steps", type=int, default=200_000,
                        help="Total training steps (default: 200000)")
    parser.add_argument("--output", type=str,
                        default=str(Path(__file__).parent.parent / "model" / "neuralbase_optimizer.onnx"),
                        help="Output ONNX model path")
    parser.add_argument("--epsilon-decay", type=int, default=EPSILON_DECAY,
                        help="Epsilon decay constant (default: EPSILON_DECAY constant in source)")
    args = parser.parse_args()
    print(f"Training for {args.steps} steps -> {args.output}")
    train(args.steps, args.output, epsilon_decay=args.epsilon_decay)
