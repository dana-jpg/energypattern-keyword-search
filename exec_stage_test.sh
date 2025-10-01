#! /bin/bash

. .venv/bin/activate
nohup python3 -m processing_pipeline.s0_noise_filtering.runner_test > data/s1_test.txt 2>&1 &


# pip3 install langchain-ollama pandas pydantic loguru tqdm pyarrow