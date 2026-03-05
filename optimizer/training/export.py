"""Export the trained DQN model to ONNX, or generate a seed model.

Seed model
----------
When no checkpoint is supplied, this script builds a minimal valid ONNX model
that implements a "smallest cardinality first" heuristic using only `onnx` and
`numpy` (PyTorch is not required for the seed).

The seed model is intentionally simple:
  Q[i] = −cardinality_feature[i]  (one Gemm layer)
so that tables with fewer rows receive higher Q-values and are selected first.
This reliably beats naive FROM-clause ordering on cost-model estimates.

Trained model export
--------------------
If a PyTorch checkpoint is specified via --checkpoint, the full 3-layer DQN is
exported instead, overwriting the seed.

Usage
-----
  # Generate seed model (no PyTorch needed):
  python export.py

  # Export trained checkpoint:
  python export.py --checkpoint /path/to/checkpoint.pt
                   --output    ../model/neuralbase_optimizer.onnx
"""

import argparse
import os
from pathlib import Path

import numpy as np
import onnx
from onnx import helper, TensorProto, numpy_helper

STATE_DIM = 72   # 8×8 adjacency + 8 cardinality features
ACTION_DIM = 8   # one action per table slot
OPSET_VERSION = 13


def build_seed_model() -> onnx.ModelProto:
    """Build a cardinality-heuristic seed model as a single Gemm layer.

    Architecture: Q = state @ W.T + b
      W[i, 64+i] = -1.0  (respond negatively to high cardinality at slot i)
      b[i]       =  1.0  (base Q-value; prevents all-zero masking)

    Effect: tables with lower log-normalised cardinality receive higher Q-values
    and are selected first by the greedy decoder in optimizer.rs.
    """
    # Weight matrix: shape [ACTION_DIM, STATE_DIM]
    W = np.zeros((ACTION_DIM, STATE_DIM), dtype=np.float32)
    for i in range(ACTION_DIM):
        if ACTION_DIM * ACTION_DIM + i < STATE_DIM:
            W[i, ACTION_DIM * ACTION_DIM + i] = -1.0

    b = np.ones(ACTION_DIM, dtype=np.float32)

    W_init = numpy_helper.from_array(W, name="W")
    b_init = numpy_helper.from_array(b, name="b")

    gemm_node = helper.make_node(
        "Gemm",
        inputs=["state", "W", "b"],
        outputs=["q_values"],
        transB=1,
    )

    graph = helper.make_graph(
        nodes=[gemm_node],
        name="dqn_seed",
        inputs=[
            helper.make_tensor_value_info(
                "state", TensorProto.FLOAT, [1, STATE_DIM]
            )
        ],
        outputs=[
            helper.make_tensor_value_info(
                "q_values", TensorProto.FLOAT, [1, ACTION_DIM]
            )
        ],
        initializer=[W_init, b_init],
    )

    model = helper.make_model(
        graph,
        opset_imports=[helper.make_opsetid("", OPSET_VERSION)],
    )
    model.ir_version = 8
    model.doc_string = (
        "NeuralBase join-order optimizer seed model. "
        "Implements cardinality-first heuristic. "
        "Replace with trained weights by running train.py."
    )
    onnx.checker.check_model(model)
    return model


def export_torch_checkpoint(checkpoint_path: str) -> onnx.ModelProto:
    """Load a PyTorch DQN checkpoint and export it to ONNX."""
    import torch
    import torch.nn as nn

    class DQN(nn.Module):
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

    model = DQN()
    state_dict = torch.load(checkpoint_path, map_location="cpu")
    model.load_state_dict(state_dict)
    model.eval()

    dummy = torch.zeros(1, STATE_DIM)
    onnx_path = "/tmp/_tmp_dqn.onnx"
    torch.onnx.export(
        model,
        dummy,
        onnx_path,
        input_names=["state"],
        output_names=["q_values"],
        opset_version=OPSET_VERSION,
    )
    return onnx.load(onnx_path)


def save_model(proto: onnx.ModelProto, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    onnx.save(proto, path)
    size_kb = Path(path).stat().st_size / 1024
    print(f"Saved ONNX model → {path}  ({size_kb:.1f} KB)")
    # Validate shapes
    proto_loaded = onnx.load(path)
    onnx.checker.check_model(proto_loaded)
    print("Model validation passed.")


if __name__ == "__main__":
    default_out = str(
        Path(__file__).parent.parent / "model" / "neuralbase_optimizer.onnx"
    )
    parser = argparse.ArgumentParser(description="Export NeuralBase ONNX model")
    parser.add_argument(
        "--checkpoint",
        type=str,
        default=None,
        help="Path to PyTorch checkpoint (.pt) to export. "
             "If omitted, generates cardinality-heuristic seed model.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=default_out,
        help=f"Output ONNX path (default: {default_out})",
    )
    args = parser.parse_args()

    if args.checkpoint:
        print(f"Exporting trained checkpoint: {args.checkpoint}")
        proto = export_torch_checkpoint(args.checkpoint)
    else:
        print("Generating cardinality-heuristic seed model (no checkpoint)…")
        proto = build_seed_model()

    save_model(proto, args.output)
