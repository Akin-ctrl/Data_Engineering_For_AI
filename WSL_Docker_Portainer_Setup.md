# Docker and Portainer Setup on Ubuntu in WSL2

This guide installs Docker and Portainer on Ubuntu running under WSL2 on Windows.

## Before You Start

Do not continue until all prerequisites below are true. If any of them are missing, WSL2 can fail with errors like:

```text
WSL2 is not supported with your current machine configuration.
Please enable the "Virtual Machine Platform" optional component
and ensure virtualization is enabled in the BIOS.
```

## Prerequisites

### 1. Confirm the hardware supports virtualization

Your CPU and firmware must support hardware virtualization.

- Intel systems: Intel VT-x or Intel Virtualization Technology
- AMD systems: AMD-V or SVM

Why this matters: WSL2 runs a lightweight virtual machine on top of Windows. If the CPU or firmware cannot virtualize, WSL2 cannot start at all.

How to confirm:

- Reboot into BIOS or UEFI setup and look for a virtualization setting.
- In Windows, open Task Manager, go to Performance, then CPU, and check whether Virtualization says Enabled.
- If your BIOS has no virtualization option and your system is old, the machine may not support WSL2.

### 2. Enable virtualization in BIOS or UEFI

Reboot into BIOS or UEFI setup and turn on the virtualization option.

- Intel: enable VT-x / Intel Virtualization Technology
- AMD: enable SVM / AMD-V

Why this matters: even if the CPU supports virtualization, the BIOS can still leave it disabled. WSL2 will fail until the firmware allows virtualization.

How to confirm:

- After saving BIOS settings, boot back into Windows.
- Open Task Manager, then Performance, then CPU.
- The Virtualization field should say Enabled.

Save the change and reboot.

### 3. Enable required Windows features

Open PowerShell as Administrator and run:

```powershell
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
```

If your Windows edition supports it and you want the full Windows virtualization stack available, you can also enable Hyper-V:

```powershell
dism.exe /online /enable-feature /featurename:Microsoft-Hyper-V-All /all /norestart
```

Why these commands are used:

- VirtualMachinePlatform is required for WSL2's virtual machine layer.
- Microsoft-Windows-Subsystem-Linux enables WSL itself.
- Hyper-V is optional on some editions, but it provides the broader Windows virtualization stack.

How to confirm:

- Open Windows Features and make sure Windows Subsystem for Linux and Virtual Machine Platform are checked.
- Reboot Windows and try `wsl --status` from PowerShell.

Restart Windows after enabling the features.

### 4. Verify Windows sees virtualization

Open Task Manager, go to Performance, and check that Virtualization says Enabled.

You can also run this in PowerShell:

```powershell
systeminfo | findstr /i "Virtualization Hyper-V Requirements"
```

Why this command is used: it prints the Windows virtualization requirements summary so you can quickly see whether the host is ready for WSL2.

How to confirm:

- The output should indicate that the machine meets the Hyper-V virtualization requirements.
- If it says virtualization is not enabled in firmware, go back to BIOS and fix that first.

### 5. Install or update WSL

From PowerShell, run:

```powershell
wsl --install
wsl --update
wsl --set-default-version 2
```

Why these commands are used:

- wsl --install sets up the WSL feature and a default Linux environment on newer Windows releases.
- wsl --update refreshes the WSL kernel and components.
- wsl --set-default-version 2 makes new Linux distros use WSL2 instead of WSL1.

If Ubuntu is already installed, make sure it uses WSL2:

```powershell
wsl -l -v
```

Why this command is used: it lists installed distros and their WSL version so you can verify Ubuntu is actually on version 2.

How to confirm:

- Ubuntu should appear in the list.
- The VERSION column should show 2.

If the Ubuntu distro shows version 1, convert it:

```powershell
wsl --set-version Ubuntu 2
```

Why this command is used: it upgrades an existing Ubuntu install from WSL1 to WSL2 so Docker can use the required kernel-backed environment.

### 6. Install Ubuntu from the Microsoft Store

Use a current Ubuntu LTS release. Open Ubuntu once so the initial Linux user account is created.

Why this step matters: Docker instructions below assume a normal Ubuntu user environment with apt available and a completed first-login setup.

How to confirm:

- Ubuntu launches successfully from the Start menu.
- You are prompted to create a Linux username and password on first launch.

### 7. Confirm Ubuntu is running on WSL2

Back in PowerShell:

```powershell
wsl -l -v
```

The Ubuntu distro must show VERSION 2.

Why this check matters: if Ubuntu is still on version 1, Docker setup will fail later even if the Linux shell opens normally.

### 8. Enable systemd in Ubuntu

Start Ubuntu and create or edit this file:

```bash
sudo nano /etc/wsl.conf
```

Add:

```ini
[boot]
systemd=true
```

Then shut down WSL from PowerShell and start Ubuntu again:

```powershell
wsl --shutdown
```

Why these steps are used: Docker is easiest to manage in WSL when systemd is available, because Docker can start as a normal Linux service.

How to confirm:

- Reopen Ubuntu after `wsl --shutdown`.
- Run `systemctl is-system-running` inside Ubuntu.
- If systemd is working, the command should return a running or degraded state instead of an error about systemd not being PID 1.

## Install Docker

### 9. Update Ubuntu packages

Run this inside Ubuntu:

```bash
sudo apt update
sudo apt upgrade -y
```

Why these commands are used:

- apt update refreshes the package index so Ubuntu knows about the latest package versions.
- apt upgrade -y applies security and compatibility updates before Docker is installed.

How to confirm:

- The commands finish without package lock or repository errors.
- You are back at the shell prompt with no failed package upgrades.

### 10. Install Docker Engine

Install the official Docker packages:

```bash
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Why these commands are used:

- The first group installs the tools needed to trust Docker's official package repository.
- The repository setup ensures you install Docker from Docker's maintained source, not a stale Ubuntu package.
- The final apt install pulls in the Docker engine, CLI, build plugins, and Compose plugin.

How to confirm:

- The install completes without repository signature errors.
- Running docker --version after install returns a Docker version string.

### 11. Start Docker and enable it at boot

Because systemd is enabled, Docker should run as a service:

```bash
sudo systemctl enable --now docker
sudo systemctl status docker --no-pager
```

Why these commands are used:

- enable --now starts Docker immediately and also configures it to start automatically in future WSL sessions.
- status confirms that the service is active and helps you see startup errors right away.

How to confirm:

- The status output should show Active: active (running).
- If it does not, read the status message before moving on.

### 12. Allow your user to run Docker without sudo

```bash
sudo usermod -aG docker $USER
newgrp docker
```

Why these commands are used:

- usermod -aG docker adds your Linux account to the docker group.
- newgrp docker refreshes the current shell so the new group membership takes effect without waiting for a full logout.

How to confirm:

- Run groups and make sure docker appears in the list.
- If it does not, close Ubuntu completely and reopen it.

### 13. Test Docker

```bash
docker run --rm hello-world
```

If this succeeds, Docker is working correctly.

Why this command is used: hello-world is the smallest safe test container. It confirms Docker can download an image, start a container, and print output.

How to confirm:

- You should see a message that Docker is installed correctly.
- If the command asks for permission denied, the docker group step was not applied correctly.

## Install Portainer

### 14. Create persistent storage for Portainer

```bash
docker volume create portainer_data
```

Why this command is used: Portainer stores its configuration and admin setup in a Docker volume so the data survives container restarts.

How to confirm:

- The command prints a volume name.
- You can run docker volume ls and see portainer_data in the list.

### 15. Run Portainer

```bash
docker run -d \
  -p 8000:8000 \
  -p 9443:9443 \
  --name portainer \
  --restart=always \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v portainer_data:/data \
  portainer/portainer-ce:latest
```

Why this command is used:

- -d runs Portainer in the background.
- -p 9443:9443 exposes the secure web UI.
- -p 8000:8000 exposes the agent or edge port used by some Portainer features.
- The Docker socket mount lets Portainer manage the local Docker engine.
- The data volume keeps Portainer settings after restart.

How to confirm:

- Run docker ps and verify the Portainer container is listed.
- If the container exits immediately, inspect it with docker logs portainer.

### 16. Open the Portainer web UI

Open:

```text
https://localhost:9443
```

Complete the first-time admin setup in the browser.

Why this step is used: the browser setup creates the initial Portainer administrator account and connects Portainer to the local Docker engine.

How to confirm:

- The Portainer login or setup page loads in the browser.
- After setup, the local Docker environment appears inside Portainer.

## Validation Checklist

Before you install anything, confirm all of these are true:

- BIOS or UEFI virtualization is enabled.
- Windows Virtual Machine Platform is enabled.
- Windows Subsystem for Linux is enabled.
- WSL reports Ubuntu as version 2.
- Ubuntu is installed and starts successfully.
- Docker starts successfully inside Ubuntu.
- Portainer starts successfully in Docker.

## If You See the WSL2 Error

If you still get the WSL2 virtualization error, stop and fix the host first:

1. Enter BIOS or UEFI and enable virtualization.
2. Make sure Virtual Machine Platform is enabled in Windows.
3. Reboot Windows completely.
4. Run `wsl -l -v` and confirm the distro is version 2.
5. Only then retry Docker and Portainer.

## Recommended Order for Learners

1. Check BIOS virtualization support.
2. Enable Virtual Machine Platform and WSL in Windows.
3. Install or upgrade WSL2.
4. Install Ubuntu.
5. Enable systemd in Ubuntu.
6. Install Docker.
7. Test Docker.
8. Start Portainer.

This order avoids the common WSL2 machine configuration error before the container setup begins.
