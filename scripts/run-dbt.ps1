Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Dbt = Join-Path $Root "dbt\.venv\Scripts\dbt.exe"
$Project = Join-Path $Root "dbt\hackathon_iot"
$Profiles = Join-Path $Root "dbt\profiles"
$Key = Join-Path $Root "cdk\snowflake-keys\rsa_key.p8"
if (-not (Test-Path $Dbt)) { throw "Missing dbt virtual environment. Recreate dbt\.venv first." }
if (-not (Test-Path $Key)) { throw "Missing local Snowflake private key." }
$env:DBT_SNOWFLAKE_ACCOUNT = Read-Host "Snowflake account identifier"
$env:DBT_SNOWFLAKE_USER = Read-Host "Snowflake service username"
$env:DBT_SNOWFLAKE_PRIVATE_KEY_PATH = $Key
$secure = Read-Host "Private-key passphrase" -AsSecureString
$ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
try {
  $env:DBT_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
  Push-Location $Project
  & $Dbt debug --profiles-dir $Profiles
  if ($LASTEXITCODE) { throw "dbt debug failed" }
  & $Dbt run --profiles-dir $Profiles
  if ($LASTEXITCODE) { throw "dbt run failed" }
  & $Dbt test --profiles-dir $Profiles
  if ($LASTEXITCODE) { throw "dbt test failed" }
  & $Dbt docs generate --profiles-dir $Profiles
  if ($LASTEXITCODE) { throw "dbt docs generate failed" }
} finally {
  Pop-Location -ErrorAction SilentlyContinue
  $env:DBT_SNOWFLAKE_ACCOUNT=$null; $env:DBT_SNOWFLAKE_USER=$null
  $env:DBT_SNOWFLAKE_PRIVATE_KEY_PATH=$null; $env:DBT_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=$null
  if ($ptr -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) }
}
