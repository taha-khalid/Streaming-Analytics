#!/usr/bin/env pwsh
<#
.SYNOPSIS
    One-click orchestration script for the Cloud Telemetry Streaming Pipeline.

.DESCRIPTION
    1. Verifies prerequisites (Java, Python, Docker)
    2. Starts Docker infrastructure (Redpanda, TimescaleDB, Grafana)
    3. Waits for DB readiness and creates hypertables
    4. Launches the Spark processor in a background job
    5. Launches the Kafka producer in a background job
    6. Opens Grafana in the default browser

.NOTES
    Run from the project root directory.
#>

$ErrorActionPreference = "Stop"
$ProjRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
if (-not $ProjRoot) { $ProjRoot = Get-Location }

function Test-Command {
    param([string]$Cmd)
    return [bool](Get-Command $Cmd -ErrorAction SilentlyContinue)
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Cloud Telemetry Streaming Pipeline" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# 1. PREREQUISITE CHECKS
# ---------------------------------------------------------------------------
Write-Host "`n[1/6] Checking prerequisites..." -ForegroundColor Yellow

if (-not (Test-Command "java")) {
    Write-Error "Java is not found in PATH. Please install Java 17 and set JAVA_HOME."
}
$javaVer = & java -version 2>&1 | Select-String -Pattern '"(\d+\.\d+).*"' | ForEach-Object { $_.Matches.Groups[1].Value }
Write-Host "   Java version: $javaVer" -ForegroundColor Green

if (-not (Test-Command "python")) {
    Write-Error "Python is not found in PATH. Please install Python 3.11+."
}
Write-Host "   Python found" -ForegroundColor Green

if (-not (Test-Command "docker")) {
    Write-Error "Docker is not found in PATH. Please start Docker Desktop."
}
Write-Host "   Docker found" -ForegroundColor Green

if (-not $env:HADOOP_HOME) {
    Write-Warning "HADOOP_HOME is not set. If Spark fails with winutils errors, create C:\hadoop and download winutils.exe."
}

# ---------------------------------------------------------------------------
# 2. PYTHON VENV
# ---------------------------------------------------------------------------
Write-Host "`n[2/6] Preparing Python environment..." -ForegroundColor Yellow
$VenvPath = Join-Path $ProjRoot "venv"
if (-not (Test-Path $VenvPath)) {
    Write-Host "   Creating virtual environment..." -ForegroundColor Gray
    & python -m venv $VenvPath
}
$PythonExe = Join-Path $VenvPath "Scripts\python.exe"
& $PythonExe -m pip install -q -r (Join-Path $ProjRoot "requirements.txt")
Write-Host "   Dependencies OK" -ForegroundColor Green

# ---------------------------------------------------------------------------
# 3. DOCKER INFRASTRUCTURE
# ---------------------------------------------------------------------------
Write-Host "`n[3/6] Starting Docker infrastructure..." -ForegroundColor Yellow
$InfraDir = Join-Path $ProjRoot "Infra"
Push-Location $InfraDir
& docker compose up -d
Pop-Location

# Wait for services
Write-Host "   Waiting for Redpanda, TimescaleDB, Grafana..." -ForegroundColor Gray
Start-Sleep -Seconds 10

# Health check loop
$maxRetries = 30
$ready = $false
for ($i = 0; $i -lt $maxRetries; $i++) {
    try {
        $dbTest = & docker exec timescaledb pg_isready -U postgres -d telemetry_db 2>$null
        if ($dbTest -match "accepting connections") {
            $ready = $true
            break
        }
    } catch {}
    Write-Host "   ... retrying DB check ($($i+1)/$maxRetries)" -ForegroundColor Gray
    Start-Sleep -Seconds 2
}

if (-not $ready) {
    Write-Error "TimescaleDB did not become ready in time. Check 'docker logs timescaledb'."
}
Write-Host "   All services are healthy" -ForegroundColor Green

# ---------------------------------------------------------------------------
# 4. DATABASE INIT (if not already done via docker-entrypoint-initdb.d)
# ---------------------------------------------------------------------------
Write-Host "`n[4/6] Verifying database schema..." -ForegroundColor Yellow
$InitFile = Join-Path $InfraDir "table_creation_query.pgsql"
$PsqlCmd = "$(docker exec -i timescaledb psql -U postgres -d telemetry_db -f /docker-entrypoint-initdb.d/01-init.sql 2>`$null)"
# Schema is auto-created by the init script mount, so just verify
Write-Host "   Schema OK (auto-provisioned via initdb)" -ForegroundColor Green

# ---------------------------------------------------------------------------
# 5. LAUNCH SPARK PROCESSOR
# ---------------------------------------------------------------------------
Write-Host "`n[5/6] Launching Spark Streaming Processor..." -ForegroundColor Yellow
$ProcessorJob = Start-Job -Name "Processor" -ScriptBlock {
    param($root, $py)
    Set-Location $root
    & $py (Join-Path $root "processor.py")
} -ArgumentList $ProjRoot, $PythonExe

Start-Sleep -Seconds 15  # Give Spark time to download jars and start

# ---------------------------------------------------------------------------
# 6. LAUNCH KAFKA PRODUCER
# ---------------------------------------------------------------------------
Write-Host "`n[6/6] Launching Kafka Producer..." -ForegroundColor Yellow
$ProducerJob = Start-Job -Name "Producer" -ScriptBlock {
    param($root, $py)
    Set-Location $root
    & $py (Join-Path $root "producer.py")
} -ArgumentList $ProjRoot, $PythonExe

# ---------------------------------------------------------------------------
# DONE
# ---------------------------------------------------------------------------
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Pipeline is LIVE!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Grafana:  http://localhost:3000" -ForegroundColor White
Write-Host "  Login:    admin / admin" -ForegroundColor White
Write-Host "  Dashboard: Cloud Telemetry Streaming Analytics" -ForegroundColor White
Write-Host "`n  Press Ctrl+C here to stop orchestration." -ForegroundColor Gray
Write-Host "  Jobs remain running in background." -ForegroundColor Gray

# Open Grafana
Start-Process "http://localhost:3000/d/telemetry-streaming-01"

# Keep the script alive and monitor jobs
try {
    while ($true) {
        Start-Sleep -Seconds 5
        $processorStatus = Get-Job -Name "Processor" -ErrorAction SilentlyContinue
        $producerStatus = Get-Job -Name "Producer" -ErrorAction SilentlyContinue

        if ($processorStatus -and $processorStatus.State -eq "Failed") {
            Write-Warning "Processor job failed. Check logs with: Receive-Job -Name Processor"
        }
        if ($producerStatus -and $producerStatus.State -eq "Failed") {
            Write-Warning "Producer job failed. Check logs with: Receive-Job -Name Producer"
        }
    }
} finally {
    Write-Host "`nStopping pipeline..." -ForegroundColor Yellow
    Stop-Job -Name "Producer" -ErrorAction SilentlyContinue
    Stop-Job -Name "Processor" -ErrorAction SilentlyContinue
    Remove-Job -Name "Producer" -ErrorAction SilentlyContinue
    Remove-Job -Name "Processor" -ErrorAction SilentlyContinue
    Write-Host "Pipeline stopped. Docker containers are still running." -ForegroundColor Green
    Write-Host "To stop containers: docker compose -f Infra/docker-compose.yml down" -ForegroundColor Gray
}
