Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root "dbt\.venv\Scripts\python.exe"
$App = Join-Path $Root "streamlit\app.py"
$Key = Join-Path $Root "cdk\snowflake-keys\rsa_key.p8"

if (-not (Test-Path $Python)) {
    throw "Missing Python environment. Recreate dbt\.venv first."
}

if (-not (Test-Path $App)) {
    throw "Missing Streamlit application: $App"
}

if (-not (Test-Path $Key)) {
    throw "Missing local Snowflake private key."
}

$env:STREAMLIT_SNOWFLAKE_ACCOUNT = Read-Host "Snowflake account identifier"
$env:STREAMLIT_SNOWFLAKE_USER = Read-Host "Snowflake service username"
$env:STREAMLIT_SNOWFLAKE_PRIVATE_KEY_PATH = $Key

$secure = Read-Host "Private-key passphrase" -AsSecureString
$ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)

try {
    $env:STREAMLIT_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = `
        [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)

    & $Python -m streamlit run $App `
        --server.address 127.0.0.1 `
        --server.port 8501 `
        --browser.gatherUsageStats false `
        --server.headless true
}
finally {
    Remove-Item Env:STREAMLIT_SNOWFLAKE_ACCOUNT -ErrorAction SilentlyContinue
    Remove-Item Env:STREAMLIT_SNOWFLAKE_USER -ErrorAction SilentlyContinue
    Remove-Item Env:STREAMLIT_SNOWFLAKE_PRIVATE_KEY_PATH -ErrorAction SilentlyContinue
    Remove-Item Env:STREAMLIT_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE -ErrorAction SilentlyContinue

    if ($ptr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
    }
}
