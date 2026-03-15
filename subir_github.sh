#!/bin/bash
# Sube el proyecto a GitHub (proyecto_transporte_global_standalone)
# Requiere: git instalado y acceso al repo (HTTPS o SSH)
set -e
cd "$(dirname "$0")"
REMOTE="https://github.com/gracobjo/proyecto_transporte_global_standalone.git"
if ! command -v git &>/dev/null; then
  echo "Instala git primero: sudo apt install git"
  exit 1
fi
if [ ! -d .git ]; then
  git init
  git add .
  git commit -m "Documentación y código: gemelo digital logístico España"
  git branch -M main
  git remote add origin "$REMOTE"
  git push -u origin main
  echo "Listo: código subido a $REMOTE"
else
  git add .
  git status
  echo "Ya hay repo. Para subir: git commit -m 'mensaje' && git push"
fi
