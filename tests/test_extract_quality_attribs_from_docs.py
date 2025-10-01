from cfg.quality_attributes import qa_sorter, QualityAttributesMap
from cfg.selected_repos import all_repos
from processing_pipeline.keyword_matching.services.KeywordExtractor import SourceCodeKeywordExtractor
from processing_pipeline.keyword_matching.services.DatasetCounter import DatasetCounter


def test_keyword_extraction_patter():
    assert SourceCodeKeywordExtractor.get_keyword_matching_pattern(sorted(["b1", "b2", r"a2\b", r"a1", "a_longer"], key=qa_sorter)).pattern == '\\b(?:(a_longer)|(a1)|(a2\\b)|(b1)|(b2))[a-z-]*\\b'
    assert SourceCodeKeywordExtractor.get_keyword_matching_pattern(sorted(["bunn", "other", "cot compon"], key=qa_sorter)).pattern == '\\b(?:(cot[a-z-]*\\b \\bcompon)|(other)|(bunn))[a-z-]*\\b'

def test_matched_keyword_iterator():
    test_qas: QualityAttributesMap = {
        "test": ["categ", r"cat\b"],
        "test2": ["kw1", "kw2", "kw3", "kw4"]
    }

    kp = SourceCodeKeywordExtractor(test_qas, all_repos[0], dataset_counter=DatasetCounter("test"))
    iter = kp.matched_keyword_iterator("category cat kw3 kw4")
    match = next(iter)
    assert match.keyword == "categ"
    assert match.qa == "test"
    assert match.matched_word == r"category"

    match = next(iter)
    assert match.keyword == "cat"
    assert match.qa == "test"
    assert match.matched_word == r"cat"

    match = next(iter)
    assert match.keyword == "kw3"
    assert match.qa == "test2"
    assert match.matched_word == "kw3"

    match = next(iter)
    assert match.keyword == "kw4"
    assert match.qa == "test2"
    assert match.matched_word == "kw4"

