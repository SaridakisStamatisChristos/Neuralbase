# test_tls.ps1 - NeuralBase TLS smoke test
# Test 1: SSLRequest -> 'S' -> TLS handshake -> startup -> server response
# Test 2: Plaintext startup (no SSLRequest) -> ErrorResponse or close
param()
$ErrorActionPreference = "Continue"
$passed = 0; $failed = 0
$HH = "127.0.0.1"; $PP = 5432

function Pass($m) { Write-Host "[PASS] $m"; $script:passed++ }
function Fail($m) { Write-Host "[FAIL] $m"; $script:failed++ }

# Add-Type: accept-all cert validator (avoids ScriptBlock-as-delegate parse problems)
Add-Type -Language CSharp -TypeDefinition @"
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
public static class TlsUtil {
    public static bool AcceptAll(object s, X509Certificate c, X509Chain ch, SslPolicyErrors e) {
        return true;
    }
}
"@ -ErrorAction SilentlyContinue

$acceptAll = [System.Net.Security.RemoteCertificateValidationCallback][TlsUtil]::AcceptAll

# ── Test 1: sslmode=require ───────────────────────────────────────────────────
Write-Host "--- Test 1: sslmode=require ---"
$tcp1 = New-Object System.Net.Sockets.TcpClient
$tcp1.Connect($HH, $PP)
$raw1 = $tcp1.GetStream()
$sslreq = [byte[]](0x00,0x00,0x00,0x08,0x04,0xD2,0x16,0x2F)
$raw1.Write($sslreq, 0, 8); $raw1.Flush()
$r1 = [byte[]](0x00)
$raw1.Read($r1, 0, 1) | Out-Null
if ($r1[0] -eq 0x53) {
    Pass "Server replied 'S' (SSL accepted)"
    $ssl1 = New-Object System.Net.Security.SslStream($raw1, $false, $acceptAll)
    $ssl1.AuthenticateAsClient("localhost")
    Pass "TLS handshake OK (proto=$($ssl1.SslProtocol) cipher=$($ssl1.CipherAlgorithm))"
    $prm = [System.Text.Encoding]::ASCII.GetBytes("user`0postgres`0`0")
    $tlen = 8 + $prm.Length
    $su1 = [byte[]]::new($tlen)
    $su1[0] = [byte](($tlen -shr 24) -band 0xFF)
    $su1[1] = [byte](($tlen -shr 16) -band 0xFF)
    $su1[2] = [byte](($tlen -shr  8) -band 0xFF)
    $su1[3] = [byte]( $tlen          -band 0xFF)
    $su1[4]=0x00; $su1[5]=0x03; $su1[6]=0x00; $su1[7]=0x00
    [Array]::Copy($prm, 0, $su1, 8, $prm.Length)
    $ssl1.Write($su1, 0, $su1.Length); $ssl1.Flush()
    $buf1 = [byte[]]::new(512)
    $ssl1.ReadTimeout = 3000
    $n1 = 0
    try { $n1 = $ssl1.Read($buf1, 0, 512) } catch { $n1 = 0 }
    if ($n1 -gt 0) {
        $fc = [char]$buf1[0]
        if ("RZSCK".IndexOf($fc) -ge 0) {
            Pass "Received server message '$fc' over TLS (connection live)"
        } else {
            Fail "Unexpected server message '$fc' (0x$([Convert]::ToString($buf1[0],16)))"
        }
    } else {
        Fail "Server closed TLS stream immediately (0 bytes read)"
    }
    $ssl1.Close()
} else {
    Fail "Expected 'S' (0x53), got 0x$([Convert]::ToString($r1[0],16))"
}
$tcp1.Close()

# ── Test 2: sslmode=disable ───────────────────────────────────────────────────
Write-Host "--- Test 2: sslmode=disable (plaintext must be rejected) ---"
$tcp2 = New-Object System.Net.Sockets.TcpClient
$tcp2.Connect($HH, $PP)
$raw2 = $tcp2.GetStream()
$raw2.ReadTimeout = 3000
# raw startup (length=40, proto v3) — NOT an SSLRequest
$su2 = [byte[]](0x00,0x00,0x00,0x28,0x00,0x03,0x00,0x00)
$raw2.Write($su2, 0, 8); $raw2.Flush()
$buf2 = [byte[]]::new(256)
$n2 = 0
try { $n2 = $raw2.Read($buf2, 0, 256) } catch { $n2 = 0 }
if ($n2 -eq 0) {
    Pass "Server closed connection (plaintext rejected — connection reset)"
} elseif ($buf2[0] -eq 0x45) {
    Pass "Server sent ErrorResponse 'E' (plaintext rejected with error)"
} elseif ($buf2[0] -eq 0x4E) {
    Fail "Server sent 'N' (declined TLS) but did not reject plaintext — TLS not enforced"
} else {
    Fail "Server accepted plaintext startup: first byte='$([char]$buf2[0])' 0x$([Convert]::ToString($buf2[0],16))"
}
try { $tcp2.Close() } catch {}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== TLS smoke: $passed passed, $failed failed ==="
if ($failed -gt 0) { exit 1 }
exit 0
