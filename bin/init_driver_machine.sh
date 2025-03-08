#!/bin/bash 

sudo apt update
sudo apt install -y golang-go openjdk-21-jdk apache2-utils python3-pip ant screen
curl https://sh.rustup.rs -sSf | sh
source $HOME/.cargo/env
python -m pip install -U pip
python -m pip install -U matplotlib
python -m pip install -U scikit-learn
python -m pip install -U requests

ant jar
./bin/build_xdn_cli.sh
cd eval/xdn_latency_proxy && cargo build --release

echo "Dont forget to initialize machines for xdn cluster using `xdnd dist-init`"