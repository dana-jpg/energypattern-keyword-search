Scripts for scraping github repositories

## file structure

- [keywords](data/keywords)
  - [matched](data/keywords/matched) Raw matched keywords 
  - [matched_o](data/keywords/matched_o) Optimized keywords
    - Reduce dataset size based on lowercase matched keywords
    - Clean sentences from newlines and repeated spaces
    - Assign ID to similar items
  - [matched_o2](data/keywords/matched_o2) Further optimized data
    - Only keep columns required for the next stage
    - Drop sentences containing 5 or fewer words
  - [merged](data/keywords/merged) Optimized keywords ([matched_o](data/keywords/matched_o)) with added fields from all the processing stages
  - [parameter_tuning](data/keywords/parameter_tuning) Subset of data for parameter tuning
  - [parameter_tuning_res](data/keywords/parameter_tuning_res) Results of parameter tuning
  - [s0_noise_filtering](data/keywords/s0_noise_filtering) Results of stage 0
  - [s0_noise_filtering_o](data/keywords/s0_noise_filtering_o) Optimized data for the stage 1
    - Only keep columns required for the next stage
  - [s1_qa_relevance_check](data/keywords/s1_qa_relevance_check) Results of stage 1
  - [s1_qa_relevance_check_o](data/keywords/s1_qa_relevance_check_o) Optimized data for the stage 2
    - Only keep columns required for the next stage
  - [s2_arch_relevance_check](data/keywords/s2_arch_relevance_check) Results of stage 2
  - [s2_arch_relevance_check_o](data/keywords/s2_arch_relevance_check_o) Optimized data for the stage 3
    - Only keep columns required for the next stage
  - [s3_tactic_extraction](data/keywords/s3_tactic_extraction) Results of stage 3