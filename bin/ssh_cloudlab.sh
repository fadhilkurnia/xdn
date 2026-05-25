#!/bin/bash
ssh -x -i $HOME/.ssh/cloudlab -o StrictHostKeyChecking=no "$@"
