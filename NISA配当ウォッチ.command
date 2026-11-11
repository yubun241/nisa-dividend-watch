#!/bin/zsh
# ダブルクリックで NISA 配当ウォッチを起動します(ブラウザが自動で開きます)
cd "$(dirname "$0")" || exit 1
source /Users/luna/python/env1/bin/activate || { echo "venv が見つかりません: /Users/luna/python/env1"; read -k1; exit 1; }
python app.py
