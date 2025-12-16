# Django project startup script (PowerShell)
# Using Gunicorn as WSGI server

# Set error action to stop on errors
$ErrorActionPreference = "Stop"

# Project root directory
$PROJECT_ROOT = "$(Split-Path -Parent $PSScriptRoot)"

# Virtual environment path
$VENV_PATH = Join-Path -Path $PROJECT_ROOT -ChildPath ".venv"
$VENV_ACTIVATE = Join-Path -Path $VENV_PATH -ChildPath "Scripts\Activate.ps1"

# Django settings module
$DJANGO_SETTINGS_MODULE = "stockDataETL.settings"

# Gunicorn configuration
$GUNICORN_HOST = "0.0.0.0"
$GUNICORN_PORT = "12037"
$GUNICORN_WORKERS = 4  # Worker processes, usually 2x CPU cores
$GUNICORN_TIMEOUT = 300
$GUNICORN_LOG_LEVEL = "info"
$GUNICORN_LOG_FILE = Join-Path -Path $PROJECT_ROOT -ChildPath "gunicorn.log"
$GUNICORN_PID_FILE = Join-Path -Path $PROJECT_ROOT -ChildPath "gunicorn.pid"

# Display startup information
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Django Project Startup Script (Gunicorn)" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Project Root: $PROJECT_ROOT" -ForegroundColor Green
Write-Host "Virtual Env: $VENV_PATH" -ForegroundColor Green
Write-Host "Listen Address: $GUNICORN_HOST`:$GUNICORN_PORT" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Cyan

# Check if virtual environment exists
if (-not (Test-Path -Path $VENV_PATH)) {
    Write-Host "Error: Virtual environment does not exist!" -ForegroundColor Red
    Write-Host "Please create virtual environment first: python -m venv .venv" -ForegroundColor Yellow
    exit 1
}

# Activate virtual environment
Write-Host "Activating virtual environment..." -ForegroundColor Yellow
try {
    & $VENV_ACTIVATE
    Write-Host "Virtual environment activated successfully!" -ForegroundColor Green
} catch {
    Write-Host "Error: Failed to activate virtual environment!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Check if gunicorn is installed
Write-Host "Checking if gunicorn is installed..." -ForegroundColor Yellow
try {
    $gunicorn_version = python -c "import gunicorn; print(gunicorn.__version__)" 2>$null
    if (-not $gunicorn_version) {
        Write-Host "Warning: gunicorn not installed, attempting to install..." -ForegroundColor Yellow
        python -m pip install gunicorn
        Write-Host "gunicorn installed successfully!" -ForegroundColor Green
    } else {
        Write-Host "gunicorn version: $gunicorn_version" -ForegroundColor Green
    }
} catch {
    Write-Host "Error: Failed to install gunicorn!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Check if project dependencies are installed
Write-Host "Checking project dependencies..." -ForegroundColor Yellow
try {
    python -c "import stockDataETL" 2>$null
    Write-Host "Project dependencies check passed!" -ForegroundColor Green
} catch {
    Write-Host "Warning: Project dependencies may not be fully installed, attempting to install..." -ForegroundColor Yellow
    try {
        # Check if requirements.txt exists
        $requirements_file = Join-Path -Path $PROJECT_ROOT -ChildPath "requirements.txt"
        if (Test-Path -Path $requirements_file) {
            python -m pip install -r $requirements_file
            Write-Host "Project dependencies installed successfully!" -ForegroundColor Green
        } else {
            Write-Host "Warning: requirements.txt file does not exist!" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "Warning: Error installing project dependencies, but will continue to start..." -ForegroundColor Yellow
        Write-Host $_.Exception.Message -ForegroundColor Yellow
    }
}

# Execute database migrations
Write-Host "Executing database migrations..." -ForegroundColor Yellow
try {
    cd $PROJECT_ROOT
    python manage.py migrate --noinput
    Write-Host "Database migrations completed!" -ForegroundColor Green
} catch {
    Write-Host "Warning: Error during database migrations, but will continue to start..." -ForegroundColor Yellow
    Write-Host $_.Exception.Message -ForegroundColor Yellow
}

# Collect static files
Write-Host "Collecting static files..." -ForegroundColor Yellow
try {
    python manage.py collectstatic --noinput
    Write-Host "Static files collection completed!" -ForegroundColor Green
} catch {
    Write-Host "Warning: Error collecting static files, but will continue to start..." -ForegroundColor Yellow
    Write-Host $_.Exception.Message -ForegroundColor Yellow
}

# Start Django development server (for Windows compatibility)
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Starting Django development server..." -ForegroundColor Yellow
Write-Host "Listen Address: $GUNICORN_HOST`:$GUNICORN_PORT" -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop server" -ForegroundColor Yellow
Write-Host "=====================================" -ForegroundColor Cyan

try {
    # Set environment variables
    $env:DJANGO_SETTINGS_MODULE = $DJANGO_SETTINGS_MODULE
    
    # Use Django's built-in runserver for Windows compatibility
    cd $PROJECT_ROOT
    python manage.py runserver $GUNICORN_HOST`:$GUNICORN_PORT
} catch {
    Write-Host "Error: Failed to start Django server!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}