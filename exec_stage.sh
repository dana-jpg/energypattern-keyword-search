#! /bin/bash

. .venv/bin/activate
#nohup python3 -m processing_pipeline.s1_qa_relevance_check.QARelevanceCheck_v2 > data/processing_logs/v2_s1_greenlab.1_5b.v6.txt 2>&1 &
#nohup python3 -m processing_pipeline.s1_qa_relevance_check.runner_2 > data/processing_logs/v2_s1_techlab.1_5b.v4.txt 2>&1 &

nohup python3 -m processing_pipeline.s2_arch_relevance_check.ArchRelevanceCheck_v2 > data/processing_logs/v2_s2_greenlab.v5.txt 2>&1 &
nohup python3 -m processing_pipeline.s2_arch_relevance_check.runner_2 > data/processing_logs/v2_s2_techlab.v5.txt 2>&1 &

nohup python3 -m processing_pipeline.s3_tactic_extraction.TacticExtraction_v2 > data/processing_logs/v2_s3_greenlab.v7.txt 2>&1 &
nohup python3 -m processing_pipeline.s3_tactic_extraction.runner_2 > data/processing_logs/v2_s3_techlab.v7.txt 2>&1 &



