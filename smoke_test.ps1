# smoke_test.ps1 — NeuralBase end-to-end smoke test (PowerShell / Windows native)
#
# Speaks the PostgreSQL wire-protocol v3 directly via TCP sockets.
# No psql, no WSL, no external tools required.
#
# Tests:
#   1. Server starts and completes startup handshake
#   2. SELECT 1 returns '1'
#   3. CREATE TABLE issues no error
#   4. INSERT with date literal ('2024-01-01') issues no error
#   5. SELECT * returns rows (row-count ≥ 1)
#   6. DELETE removes a row
#   7. UPDATE modifies a row
#   8. TPC-H lineitem SELECT returns at least 1 row
#   9. Server survives restart; CREATE TABLE schema persists
#
# Usage:
#   .\smoke_test.ps1              # debug build
#   .\smoke_test.ps1 -Release     # release build

param([switch]$Release)

$ErrorActionPreference = "Continue"  # external tools write to stderr legitimately

# ── Config ────────────────────────────────────────────────────────────────
$Port   = 5499
$Host_  = "127.0.0.1"
$DbDir  = $null
$BinProc = $null
$Failed  = 0

# ── Cleanup ───────────────────────────────────────────────────────────────
function Cleanup {
    if ($BinProc -and !$BinProc.HasExited) {
        try { Stop-Process -Id $BinProc.Id -Force -ErrorAction SilentlyContinue } catch {}
        Start-Sleep -Milliseconds 200
    }
    if ($DbDir -and (Test-Path $DbDir)) {
        Remove-Item -Recurse -Force $DbDir -ErrorAction SilentlyContinue
    }
}

# ── Build ─────────────────────────────────────────────────────────────────
$BuildDir = ".\target\debug"
if ($Release) {
    Write-Host "==> Building (release)..."
    $null = cargo build --release 2>&1
    $BuildDir = ".\target\release"
} else {
    Write-Host "==> Building (debug)..."
    $null = cargo build 2>&1
}

$Binary = "$BuildDir\neuralbase.exe"
if (-not (Test-Path $Binary)) {
    Write-Error "Binary not found: $Binary"
    exit 1
}

# ── PostgreSQL Wire Protocol helpers ──────────────────────────────────────

function Int32BE([int]$v) {
    $b = [System.BitConverter]::GetBytes([int]$v)
    [Array]::Reverse($b)
    return $b
}

function MakeStartup {
    # protocol = 196608 = 0x00030000 (v3.0) — bytes in big-endian order
    $proto = [byte[]](0x00, 0x03, 0x00, 0x00)
    $body  = $proto + [System.Text.Encoding]::ASCII.GetBytes("user`0postgres`0`0")
    $len   = Int32BE($body.Length + 4)  # length includes itself
    return $len + $body
}

# Read exactly N bytes from stream, with timeout.
function ReadExact([System.Net.Sockets.NetworkStream]$stream, [int]$count) {
    $buf = New-Object byte[] $count
    $got = 0
    $deadline = [System.DateTime]::UtcNow.AddSeconds(5)
    while ($got -lt $count -and [System.DateTime]::UtcNow -lt $deadline) {
        if ($stream.DataAvailable) {
            $n = $stream.Read($buf, $got, $count - $got)
            if ($n -le 0) { break }
            $got += $n
        } else {
            Start-Sleep -Milliseconds 10
        }
    }
    if ($got -lt $count) { return $null }
    return $buf
}

# Read one PG backend message: returns [type, payload_bytes]
function ReadMsg([System.Net.Sockets.NetworkStream]$stream) {
    $hdr = ReadExact $stream 5
    if ($null -eq $hdr) { return $null }
    $type    = [char]$hdr[0]
    $bodyLen = ([int]$hdr[1] -shl 24) -bor ([int]$hdr[2] -shl 16) -bor ([int]$hdr[3] -shl 8) -bor [int]$hdr[4]
    $payLen  = $bodyLen - 4
    if ($payLen -lt 0) { return @($type, @()) }
    if ($payLen -eq 0) { return @($type, @()) }
    $payload = ReadExact $stream $payLen
    if ($null -eq $payload) { return $null }
    return @($type, $payload)
}

# Drain messages until ReadyForQuery ('Z').
function DrainToReady([System.Net.Sockets.NetworkStream]$stream) {
    $deadline = [System.DateTime]::UtcNow.AddSeconds(8)
    while ([System.DateTime]::UtcNow -lt $deadline) {
        $msg = ReadMsg $stream
        if ($null -eq $msg) { return $false }
        if ($msg[0] -eq 'Z') { return $true }
    }
    return $false
}
function MakeQuery([string]$sql) {
    # Format: 'Q'[1] + len[4 BE, includes itself] + sql\0
    $body    = [System.Text.Encoding]::ASCII.GetBytes($sql + "`0")
    $lenBody = Int32BE($body.Length + 4)
    return [byte[]](0x51) + $lenBody + $body   # 0x51 = 'Q'
}

function StartServer([string]$dbPath) {
    # Set env vars on the current PowerShell process — they are inherited by child processes.
    $env:LISTEN_ADDR = "${Host_}:${Port}"
    $env:DB_PATH     = $dbPath
    $proc = Start-Process -FilePath (Resolve-Path $Binary).Path `
                          -NoNewWindow -PassThru
    # Unset so other processes aren't affected by stale values.
    Remove-Item Env:LISTEN_ADDR -ErrorAction SilentlyContinue
    Remove-Item Env:DB_PATH     -ErrorAction SilentlyContinue
    return $proc
}

function WaitReady([int]$timeoutMs = 10000) {
    $deadline = [System.DateTime]::UtcNow.AddMilliseconds($timeoutMs)
    while ([System.DateTime]::UtcNow -lt $deadline) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $tcp.Connect($Host_, $Port)
            $tcp.Close()
            return $true
        } catch {
            Start-Sleep -Milliseconds 100
        }
    }
    return $false
}

function PgSession([scriptblock]$body) {
    $tcp    = New-Object System.Net.Sockets.TcpClient
    $tcp.Connect($Host_, $Port)
    $stream = $tcp.GetStream()
    try {
        # Send startup
        $startup = MakeStartup
        $stream.Write($startup, 0, $startup.Length)
        $stream.Flush()

        # Drain until ReadyForQuery ('Z')
        if (-not (DrainToReady $stream)) {
            throw "Server did not send ReadyForQuery after startup"
        }

        # Execute queries
        $result = & $body $stream
    } finally {
        $stream.Close()
        $tcp.Close()
    }
    return $result
}

function RunQuery([System.Net.Sockets.NetworkStream]$stream, [string]$sql) {
    $q = MakeQuery $sql
    $stream.Write($q, 0, $q.Length)
    $stream.Flush()

    $response = ""
    $hasError = $false
    $dataRows = 0
    $deadline = [System.DateTime]::UtcNow.AddSeconds(10)

    while ([System.DateTime]::UtcNow -lt $deadline) {
        $msg = ReadMsg $stream
        if ($null -eq $msg) { break }
        $type    = $msg[0]
        $payload = $msg[1]

        switch ($type) {
            'Z' { # ReadyForQuery — done
                return [PSCustomObject]@{
                    Response = $response.Trim()
                    HasError  = $hasError
                    DataRows  = $dataRows
                    TimedOut  = $false
                }
            }
            'E' { # ErrorResponse
                $errBytes = $payload | Where-Object { $_ -ge 32 }
                $errMsg = [System.Text.Encoding]::ASCII.GetString([byte[]]$errBytes)
                $hasError = $true
                $response += "ERROR: $errMsg "
            }
            'D' { $dataRows++ }   # DataRow
            'C' {                  # CommandComplete tag
                $tag = [System.Text.Encoding]::ASCII.GetString([byte[]]$payload)
                $response += $tag.TrimEnd([char]0)
            }
            'T' {}  # RowDescription — ignore
            default {}
        }
    }

    return [PSCustomObject]@{
        Response = $response.Trim()
        HasError  = $hasError
        DataRows  = $dataRows
        TimedOut  = $true
    }
}

function Assert-Pass([string]$label, [bool]$cond, [string]$detail = "") {
    if ($cond) {
        Write-Host "  PASS  $label" -ForegroundColor Green
    } else {
        Write-Host "  FAIL  $label$(if ($detail) { ": $detail" })" -ForegroundColor Red
        $script:Failed++
    }
}

# ── Main ──────────────────────────────────────────────────────────────────
try {
    $DbDir = New-TemporaryFile | ForEach-Object { Remove-Item $_; mkdir $_ } | Select-Object -ExpandProperty FullName

    Write-Host ""
    Write-Host "==> Starting NeuralBase on ${Host_}:${Port} (DB_PATH=$DbDir)..."
    $BinProc = StartServer $DbDir

    if (-not (WaitReady 10000)) {
        Write-Error "Server did not become ready within 10 s"
        exit 1
    }
    Write-Host "    Server ready (PID $($BinProc.Id))"

    # ── Test suite ────────────────────────────────────────────────────────
    PgSession {
        param($stream)

        Write-Host ""
        Write-Host "--- Round 1: Basic connectivity + DML ---"

        # 1. SELECT 1
        $r = RunQuery $stream "SELECT 1"
        Assert-Pass "SELECT 1 returns row" ($r.DataRows -ge 1) $r.Response
        Assert-Pass "SELECT 1 no error"    (-not $r.HasError)   $r.Response

        # 2. CREATE TABLE
        $r = RunQuery $stream "CREATE TABLE orders (order_id BIGINT, event_date DATE, amount DOUBLE PRECISION, note VARCHAR)"
        Assert-Pass "CREATE TABLE no error" (-not $r.HasError) $r.Response
        Assert-Pass "CREATE TABLE response" ($r.Response -like "CREATE TABLE*") $r.Response

        # 3. INSERT with date literal coercion
        $r = RunQuery $stream "INSERT INTO orders VALUES (1, '2024-01-01', 99.50, 'first')"
        Assert-Pass "INSERT row 1 no error"  (-not $r.HasError) $r.Response
        Assert-Pass "INSERT response INSERT 0 1" ($r.Response -like "INSERT 0 1*") $r.Response

        $r = RunQuery $stream "INSERT INTO orders VALUES (2, '2024-06-15', 250.00, 'second')"
        Assert-Pass "INSERT row 2 no error" (-not $r.HasError) $r.Response

        $r = RunQuery $stream "INSERT INTO orders VALUES (3, '2025-12-31', 1.00, 'third')"
        Assert-Pass "INSERT row 3 no error" (-not $r.HasError) $r.Response

        # 4. SELECT * returns all 3 rows
        $r = RunQuery $stream "SELECT * FROM orders"
        Assert-Pass "SELECT returns 3 rows" ($r.DataRows -eq 3) "got $($r.DataRows) rows"
        Assert-Pass "SELECT no error"       (-not $r.HasError)  $r.Response

        # 5. DELETE removes 1 row
        $r = RunQuery $stream "DELETE FROM orders WHERE order_id = 1"
        Assert-Pass "DELETE no error"         (-not $r.HasError)   $r.Response
        Assert-Pass "DELETE response DELETE 1" ($r.Response -like "DELETE 1*") $r.Response

        $r = RunQuery $stream "SELECT * FROM orders"
        Assert-Pass "SELECT 2 rows after delete" ($r.DataRows -eq 2) "got $($r.DataRows) rows"

        # 6. UPDATE modifies 1 row
        $r = RunQuery $stream "UPDATE orders SET amount = 300.00 WHERE order_id = 2"
        Assert-Pass "UPDATE no error"         (-not $r.HasError) $r.Response
        Assert-Pass "UPDATE response UPDATE 1" ($r.Response -like "UPDATE 1*") $r.Response

        # 7. TPC-H lineitem
        $r = RunQuery $stream "SELECT * FROM lineitem LIMIT 5"
        Assert-Pass "lineitem SELECT returns rows" ($r.DataRows -ge 1) "got $($r.DataRows) rows"
        Assert-Pass "lineitem SELECT no error"     (-not $r.HasError)  $r.Response

        # 8. Malformed SQL returns error (not a crash)
        $r = RunQuery $stream "EXPLODE INTO orders"
        Assert-Pass "malformed SQL returns error" ($r.HasError) $r.Response
    } | Out-Null

    # ── Restart test (catalog durability) ─────────────────────────────────
    Write-Host ""
    Write-Host "--- Round 2: Catalog durability across restart ---"
    if ($BinProc -and !$BinProc.HasExited) {
        Stop-Process -Id $BinProc.Id -Force
        Start-Sleep -Milliseconds 500
    }

    $BinProc = StartServer $DbDir
    if (-not (WaitReady 10000)) {
        Write-Host "  FAIL  Server did not restart" -ForegroundColor Red
        $Failed++
    } else {
        Write-Host "    Server restarted (PID $($BinProc.Id))"
        PgSession {
            param($stream)
            # orders table must still exist after restart (schema in RocksDB)
            $r = RunQuery $stream "SELECT * FROM orders"
            # Main test: no TableNotFound error (schema was persisted)
            Assert-Pass "orders table survived restart (no error)" (-not $r.HasError) $r.Response
        } | Out-Null
    }

    # ── Summary ───────────────────────────────────────────────────────────
    Write-Host ""
    if ($Failed -eq 0) {
        Write-Host "============================================" -ForegroundColor Green
        Write-Host "  All smoke tests PASSED"                    -ForegroundColor Green
        Write-Host "============================================" -ForegroundColor Green
        exit 0
    } else {
        Write-Host "============================================" -ForegroundColor Red
        Write-Host "  $Failed smoke test(s) FAILED"              -ForegroundColor Red
        Write-Host "============================================" -ForegroundColor Red
        exit 1
    }
} finally {
    Cleanup
}
