This directory has files for helping test the package on Windows using Docker.

WARNING: Substantial improvements could be made to this approach; unfortunately,
there's limited documentation online for how to setup and manage Docker images
that run Windows inside of the containers. Feel free to submit a PR and/or open
an issue in GitHub if you know of a better way to do this.

Docker Setup
============

First, you'll need to install Docker Desktop on Windows, and reconfigure it to
use Windows containers instead of Linux containers. It can only fully support
one type at a time.

- Download and install [Docker
  Desktop](https://www.docker.com/products/docker-desktop/). It's fine to
  install the version that supports WSL2, but we will be using the Hyper-V
  backend.

- In and *administrator* PowerShell, run the following. Reboot or log out and
  back in afterwards if it asks you to.

      Enable-WindowsOptionalFeature -Online \
           -FeatureName $("Microsoft-Hyper-V", "Containers") -All

- Launch Docker Desktop.

- Find the Docker Desktop icon in the Windows System Tray (the little icons next
  to the clock). Right click on it. Select "Switch to Windows Containers..." on
  the menu. Reboot or log out and back in afterwards if it asks you to.

  - Do not skip this step. If you don't switch modes, the Docker images you
    create will launch Linux containers, not Windows containers. If you're
    trying to run or test the code on WSL under Windows, see the general
    "Contributing to Pyrseus" documentation instead of this file.

Creating and Testing the Docker Image
=====================================

Now we need to setup a Docker image.

- If your clone of this library is on a WSL partition, you need to copy it to
  an NTFS drive. As of Docker 4.72.2, Docker fails to create a proper image if
  it's copying data from WSL2. Here is a hacky way that does not require
  installing Git on the host Windows system.

       # In a WSL2 prompt. If you have Git installed on Windows, you can use
       # Git to push and pull between your WSL2 clone and your Windows clone.
       # Or if you prefer driving this from the Windows side, you could use
       # Windows' robocopy instead of Linux's rsync. But don't copy the
       # hidden .tox/ directory.
       ~% cd /path/to/your/clone/on/wsl2
       wsl2% mkdir -p /mnt/c/pyrseus
       wsl2% rsync --progress \
         --exclude=.tox --exclude=.git --exclude=docs \
         --exclude=__pycache__ --exclude=dist \
         --exclude=.ruff_cache --exclude=ipynb_checkpoints \
         --exclude=.pytest_cache \
         -a --delete-after --delete-excluded . /mnt/c/pyrseus

- Ensure Docker Desktop is running on Windows (not WSL) and that the Docker
  service is active.

- Now we build the Docker image. This must be done in an *administrator*
  Windows `cmd` or `powershell` prompt, not in a WSL shell.

       # In a PowerShell or CMD.exe prompt. NOT in a WSL shell!
       # Must be on an NTFS drive, not a WSL2 mount!
       PS C:\pyrseus> docker build -t pyrseus-py311-win32 -f .\docker-win32\Dockerfile .

- Now you're ready to run tests your newly created Docker container.

       # In a PowerShell or CMD.exe prompt. NOT in a WSL shell!
       # Must be on an NTFS drive, not a WSL2 mount!
       PS C:\pyrseus> docker run -it --rm pyrseus-py311-win32 docker-win32\test.bat

- If you'd like to troubleshoot further, replace `docker-win32\test.bat` with
  `cmd` or `powershell`. E.g. to test only the `pyrseus-ctx` subpackage:

       PS C:\pyrseus> docker run -it --rm pyrseus-py311-win32 cmd
       C:\usr\src\myapp> cd pyrseus-ctx
       C:\usr\src\myapp> tox

- To test any changes made on the WSL side, repeat all of the prior
  instructions in this section.
