# Get the current script's directory
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = (Get-Item $scriptPath).Parent.FullName

# Create PowerShell profile if it doesn't exist
$profilePath = $PROFILE
if (-not (Test-Path $profilePath)) {
    New-Item -ItemType File -Path $profilePath -Force
    Write-Host "Created PowerShell profile at: $profilePath"
}

# Add project path to PowerShell profile
$profileContent = @"

# Tableau Application Environment Setup
`$env:PYTHONPATH = "$projectRoot\src;`$env:PYTHONPATH"

# Optional: Add project's Python scripts to PATH
`$env:PATH = "$projectRoot\scripts;`$env:PATH"

"@

# Check if the content is already in the profile
$currentProfile = Get-Content $profilePath -Raw
if (-not $currentProfile.Contains("Tableau Application Environment Setup")) {
    Add-Content -Path $profilePath -Value $profileContent
    Write-Host "Added Tableau Application environment setup to PowerShell profile"
} else {
    Write-Host "Tableau Application environment setup already exists in profile"
}

Write-Host "`nSetup complete! Please restart your PowerShell session for changes to take effect."
Write-Host "You can now import the Tableau Application package from anywhere." 