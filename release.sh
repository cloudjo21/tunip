#!/bin/bash
set -e

PKG_NAME=tunip

# pyproject.toml에서 버전 읽기
VERSION=$(python3 -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")

echo "Releasing ${PKG_NAME} v${VERSION}..."

# 빌드
uv build

# git tag & push
git tag "v${VERSION}"
git push origin "v${VERSION}"

echo "${PKG_NAME} v${VERSION} release completed!"
