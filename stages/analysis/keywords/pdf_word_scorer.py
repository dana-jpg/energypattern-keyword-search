import os
import PyPDF2
import nltk
from nltk.corpus import stopwords, wordnet
from nltk.stem import PorterStemmer
from nltk import WordNetLemmatizer, ngrams as nltk_ngrams
from nltk.collocations import BigramAssocMeasures, BigramCollocationFinder
from nltk.probability import FreqDist

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import pandas as pd
import re
from collections import defaultdict
import shelve  # For caching
import hashlib  # For content hashing
from pathlib import Path  # For easier path manipulation
import math  # For log in c-TF-IDF

from cfg.patterns import patterns  # Assuming this file exists and contains the map

# --- Configuration ---
SEED_WORDS_RAW_MAP = patterns
USE_STEMMING = True
PREPROCESSING_VERSION = "v1.2"  # Incremented due to c-TF-IDF logic and potential changes
CACHE_DIR = "../../../.cache/doc_analysis_cache"  # Changed cache dir name slightly for clarity
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)


# --- NLTK Resource Download ---
def ensure_nltk_resources():
    resources = [('corpora/wordnet', 'wordnet'),
                 ('corpora/stopwords', 'stopwords'),
                 ('tokenizers/punkt', 'punkt')]
    for path, name in resources:
        try:
            nltk.data.find(path)
        except nltk.downloader.DownloadError:
            print(f"Downloading NLTK resource: {name}")
            nltk.download(name)


# --- Global NLP Objects ---
if USE_STEMMING:
    stemmer = PorterStemmer()
else:
    lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))


# --- Helper Functions (find_documents_by_qa, extract_text_from_document, get_file_hash,
#                      preprocess_text_and_map, get_synonyms_wordnet, process_seed_keywords,
#                      format_original_words, get_term_frequencies,
#                      calculate_corpus_tfidf_with_components,
#                      calculate_qa_level_tfidf_aggregates,
#                      prepare_seed_keywords_sheet_data,
#                      extract_ngrams, find_collocations_general, find_collocations_with_seeds
#                      are assumed to be defined as in your previous complete script,
#                      with TXT file support already integrated in find_documents_by_qa and extract_text_from_document)
#                     I will redefine find_documents_by_qa and extract_text_from_document for completeness.

def find_keyword_matches(term_series, seed_keyword_series):
    """
    Finds which seed keywords are contained within terms and vice-versa.

    Args:
        term_series (pd.Series): Series of processed terms (e.g., from QA_Refined_cTFIDF).
        seed_keyword_series (pd.Series): Series of processed seed keywords.

    Returns:
        tuple: (
            term_to_seed_matches (pd.DataFrame): DataFrame with 'Term (Processed)',
                                                 'Matching Seed Count', 'Matching Seeds'.
            seed_to_term_matches (dict): Dictionary mapping seed keywords to a list of
                                         terms they were found in.
        )
    """
    # Ensure seed keywords are unique and non-empty for efficient processing
    unique_seeds = seed_keyword_series.dropna().unique()
    unique_seeds = [s for s in unique_seeds if s.strip()]  # Filter out empty strings

    term_matches_data = []
    seed_to_term_lookup = defaultdict(set)  # Using set to avoid duplicate terms per seed

    if not unique_seeds:  # No seeds to match against
        for term in term_series.dropna().unique():
            term_matches_data.append({
                'Term (Processed)': term,
                'Matching Seed Count': 0,
                'Matching Seeds': ""
            })
        return pd.DataFrame(term_matches_data), {}

    for term in term_series.dropna().unique():  # Iterate unique terms to avoid redundant calculations
        current_matching_seeds = []
        if not term.strip():  # Skip empty terms
            continue
        for seed in unique_seeds:
            if seed in term:  # Check if seed is a substring of term
                current_matching_seeds.append(seed)
                seed_to_term_lookup[seed].add(term)

        term_matches_data.append({
            'Term (Processed)': term,
            'Matching Seed Count': len(current_matching_seeds),
            'Matching Seeds': ", ".join(sorted(list(set(current_matching_seeds))))  # Ensure unique, sorted seeds
        })

    # Convert the set of terms in seed_to_term_lookup to sorted comma-separated strings
    final_seed_to_term_lookup = {
        seed: ", ".join(sorted(list(terms)))
        for seed, terms in seed_to_term_lookup.items()
    }

    return pd.DataFrame(term_matches_data), final_seed_to_term_lookup

def find_documents_by_qa(base_dir):
    qa_to_documents = defaultdict(list)
    if not os.path.isdir(base_dir):
        print(f"Error: Base document directory '{base_dir}' not found.")
        return qa_to_documents
    for dir_item_name in os.listdir(base_dir):
        item_path = os.path.join(base_dir, dir_item_name)
        if os.path.isdir(item_path):
            matched_qa_key = next((k for k in SEED_WORDS_RAW_MAP if k.lower() == dir_item_name.lower()), None)
            if matched_qa_key:
                # print(f"Found QA directory: {dir_item_name} (mapped to key: '{matched_qa_key}')")
                for root, _, files in os.walk(item_path):
                    for file in files:
                        file_lower = file.lower()
                        if file_lower.endswith(".pdf") or file_lower.endswith(".txt"):
                            doc_path = os.path.join(root, file)
                            qa_to_documents[matched_qa_key].append(doc_path)
            # else:
            # print(f"Skipping directory '{dir_item_name}' as it's not a recognized QA in SEED_WORDS_RAW_MAP.")
    return qa_to_documents


def extract_text_from_pdf(pdf_path):  # Copied from previous for self-containment
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                try:
                    text += page.extract_text() + "\n"
                except:
                    pass
        return text
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}"); return None


def extract_text_from_document(doc_path):
    if doc_path.lower().endswith(".pdf"):
        return extract_text_from_pdf(doc_path)
    elif doc_path.lower().endswith(".txt"):
        try:
            with open(doc_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except Exception as e:
            print(f"Error reading TXT {doc_path}: {e}"); return None
    print(f"Unsupported file type: {doc_path}");
    return None


def get_file_hash(filepath):  # Copied
    h = hashlib.md5()
    try:
        with open(filepath, 'rb') as f:
            while True:
                buf = f.read(65536)
                if not buf: break
                h.update(buf)
        return h.hexdigest()
    except:
        return None


def preprocess_text_and_map(text, pdf_path_for_log=""):  # Copied
    if not text: return [], defaultdict(set)
    text = re.sub(r'([a-z])-\n([a-z])', r'\1\2', text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r'-\n', '', text, flags=re.IGNORECASE | re.MULTILINE)
    text_lower = text.lower()
    text_for_tokenization = re.sub(r'[^a-z\s-]', '', text_lower)
    raw_tokens = nltk.word_tokenize(text_for_tokenization)
    final_processed_tokens, stem_to_raw_map = [], defaultdict(set)
    for token in raw_tokens:
        cleaned = token.strip('-')
        if not cleaned or all(c == '-' for c in token) or cleaned in stop_words or len(cleaned) <= 2: continue
        processed = stemmer.stem(cleaned) if USE_STEMMING else lemmatizer.lemmatize(cleaned)
        final_processed_tokens.append(processed);
        stem_to_raw_map[processed].add(cleaned)
    return final_processed_tokens, stem_to_raw_map


def format_original_words(original_set):  # Copied
    if not original_set: return ""
    if len(original_set) == 1: return list(original_set)[0]
    return f"({', '.join(sorted(list(original_set)))})"


def space_tokenizer(text):
    return text.split()


def get_term_frequencies(processed_doc_text_list):  # Copied
    tf_vectorizer = CountVectorizer(ngram_range=(1, 1), tokenizer=space_tokenizer)
    tf_matrix = tf_vectorizer.fit_transform(processed_doc_text_list)
    return tf_matrix, tf_vectorizer


def get_synonyms_wordnet(word, pos=None, max_synonyms_per_pos=3):  # Copied
    synonyms = set()
    pos_tags_to_try = [wordnet.NOUN, wordnet.VERB, wordnet.ADJ, wordnet.ADV]
    if pos: pos_tags_to_try = [pos]
    for wn_pos in pos_tags_to_try:
        synsets = wordnet.synsets(word, pos=wn_pos)
        count = 0
        for synset in synsets:
            for lemma in synset.lemmas():
                synonym = lemma.name().replace('_', ' ')
                if synonym.lower() != word.lower():
                    synonyms.add(synonym);
                    count += 1
                    if count >= max_synonyms_per_pos: break
            if count >= max_synonyms_per_pos: break
    return list(synonyms)


def process_seed_keywords(raw_keywords, expand_synonyms=False, max_synonyms_per_seed=3):  # Copied
    all_keywords = set(k.lower() for k in raw_keywords)
    if expand_synonyms:
        expanded = set(all_keywords)
        for seed in raw_keywords:
            for syn in get_synonyms_wordnet(seed.lower(), max_synonyms_per_pos=max_synonyms_per_seed):
                expanded.add(syn.lower())
        all_keywords = expanded
    if not all_keywords: return set()
    tokens, _ = preprocess_text_and_map(" ".join(all_keywords))
    return set(tokens)


def calculate_corpus_tfidf_with_components(
        all_processed_docs_texts,
        all_stem_to_raw_maps_list,  # Needed if target_doc_indices is not empty
        target_doc_indices,  # Will be [] during the global fitting call from main
        # These are passed as None by main during the global fitting call
        fitted_tfidf_vectorizer_in=None,
        fitted_tf_vectorizer_in=None,
        global_term_idf_map_in=None,
        global_term_doc_count_map_in=None
):
    num_total_corpus_docs = len(all_processed_docs_texts)
    results_dfs = []  # Initialize for the return value

    # --- Stage 1: Ensure TF Vectorizer and Global Doc Count Map are ready ---
    # Use local variables for fitting, then assign to return variables
    current_fitted_tf_vectorizer = fitted_tf_vectorizer_in
    current_global_term_doc_count_map = global_term_doc_count_map_in

    if current_fitted_tf_vectorizer is None or current_global_term_doc_count_map is None:
        print(f"Fitting CountVectorizer for TF and global doc counts...")
        if not all_processed_docs_texts or all(not s for s in all_processed_docs_texts):
            print("Error: No processable text found for CountVectorizer fitting.")
            return results_dfs, None, None, None, None  # Early exit: cannot proceed

        tf_matrix_full_corpus, temp_tf_vectorizer = get_term_frequencies(all_processed_docs_texts)

        if temp_tf_vectorizer and hasattr(temp_tf_vectorizer, 'vocabulary_') and temp_tf_vectorizer.vocabulary_:
            current_fitted_tf_vectorizer = temp_tf_vectorizer
            temp_map = defaultdict(int)
            tf_vocab = current_fitted_tf_vectorizer.vocabulary_
            doc_counts_per_term_arr = tf_matrix_full_corpus.sum(
                axis=0).A1  # How many times term appears across all docs
            # For global_doc_count_map, we need # of DOCS term appears in.
            # tf_matrix_full_corpus is docs x terms. Non-zero count per column is doc freq.
            binary_tf_matrix = tf_matrix_full_corpus.astype('bool').astype('int')
            doc_freq_per_term_arr = binary_tf_matrix.sum(axis=0).A1

            for term, index in tf_vocab.items():
                temp_map[term] = int(doc_freq_per_term_arr[index])
            current_global_term_doc_count_map = temp_map
        else:
            print("Error: CountVectorizer fitting failed or resulted in empty vocabulary.")
            return results_dfs, None, None, None, None  # Critical failure

    # --- Stage 2: Ensure TF-IDF Vectorizer and Global IDF Map are ready ---
    current_fitted_tfidf_vectorizer = fitted_tfidf_vectorizer_in
    current_global_term_idf_map = global_term_idf_map_in

    if current_fitted_tfidf_vectorizer is None or current_global_term_idf_map is None:
        print(f"Fitting TF-IDF vectorizer for TF-IDF scores and global IDF map...")
        if not current_fitted_tf_vectorizer:  # Dependency check
            print("Error: Cannot fit TF-IDF because TF vectorizer is not available.")
            return results_dfs, current_fitted_tf_vectorizer, None, current_global_term_doc_count_map, None

        # If you pass vocabulary, min_df and max_df in TfidfVectorizer are IGNORED.
        # If you want min_df/max_df to take effect, you should apply them to CountVectorizer
        # and then TfidfVectorizer will inherit that (smaller) vocabulary.
        # For now, assuming you want TF-IDF on the full vocab from TF vectorizer.
        tfidf_vectorizer = TfidfVectorizer(ngram_range=(1, 1),
                                           # min_df=2, # This would be ignored if vocabulary is passed
                                           # max_df=0.5, # This would be ignored if vocabulary is passed
                                           vocabulary=current_fitted_tf_vectorizer.vocabulary_,
                                           smooth_idf=True, use_idf=True)  # Ensure IDF is calculated
        try:
            if not all_processed_docs_texts or all(not s for s in all_processed_docs_texts):
                raise ValueError("Cannot fit TfidfVectorizer on empty or all-empty documents.")

            corpus_tfidf_matrix_fit = tfidf_vectorizer.fit_transform(all_processed_docs_texts)  # Fit AND transform

            if not hasattr(tfidf_vectorizer, 'idf_') or tfidf_vectorizer.idf_ is None:
                raise ValueError("TF-IDF fitting did not produce IDF_ attribute.")

            current_fitted_tfidf_vectorizer = tfidf_vectorizer
            temp_map = defaultdict(float)
            idf_vocab = current_fitted_tfidf_vectorizer.vocabulary_
            idf_scores_arr = current_fitted_tfidf_vectorizer.idf_
            for term, index in idf_vocab.items():
                temp_map[term] = idf_scores_arr[index]
            current_global_term_idf_map = temp_map

        except ValueError as e:
            print(f"Could not fit TF-IDF vectorizer. Reason: {e}")
            # Return what we have so far, TFIDF parts will be None
            return results_dfs, current_fitted_tf_vectorizer, None, current_global_term_doc_count_map, None

            # --- Stage 3: Calculate scores for target_doc_indices (if any) ---
    # This part uses the vectorizers and maps that are now guaranteed to be fitted (or were passed in)
    if not all([current_fitted_tf_vectorizer, current_fitted_tfidf_vectorizer,
                current_global_term_doc_count_map is not None, current_global_term_idf_map is not None]):
        print("Error: Vectorizers or global maps not pre-fitted. This function expects them.")  # This is your error
        # This should not be reached if the above fitting logic is correct and exits early on failure.
        # However, if it is reached, it means the variables passed IN were None, and fitting also failed to populate them.
        return results_dfs, current_fitted_tf_vectorizer, current_fitted_tfidf_vectorizer, current_global_term_doc_count_map, current_global_term_idf_map

    # If target_doc_indices is empty (like during the fitting call from main), this loop won't run.
    # If it's not empty (like when called per QA), it will generate the per-doc DFs.
    if target_doc_indices:
        print(f"Calculating TF/IDF components for {len(target_doc_indices)} target documents...")
        # We need the transformed matrices for the *entire corpus* to slice from
        # If they were fitted above, corpus_tfidf_matrix_fit can be reused.
        # If vectorizers were passed in, we need to transform.
        if 'corpus_tfidf_matrix_fit' not in locals():  # If not fitted in this call
            corpus_tfidf_matrix_for_targets = current_fitted_tfidf_vectorizer.transform(all_processed_docs_texts)
            tf_matrix_for_targets = current_fitted_tf_vectorizer.transform(all_processed_docs_texts)
        else:  # Was fitted in this call
            corpus_tfidf_matrix_for_targets = corpus_tfidf_matrix_fit  # Use the one from fitting
            # We also need tf_matrix_full_corpus if it was fitted here
            if 'tf_matrix_full_corpus' not in locals() and current_fitted_tf_vectorizer:  # Should have been fitted if tfidf was
                tf_matrix_for_targets = current_fitted_tf_vectorizer.transform(all_processed_docs_texts)
            elif 'tf_matrix_full_corpus' in locals():
                tf_matrix_for_targets = tf_matrix_full_corpus
            else:  # Should not happen
                print("Error: TF matrix not available for target doc analysis.")
                return results_dfs, current_fitted_tf_vectorizer, current_fitted_tfidf_vectorizer, current_global_term_doc_count_map, current_global_term_idf_map

        tfidf_feature_names = current_fitted_tfidf_vectorizer.get_feature_names_out()
        # tf_feature_names should align due to shared vocabulary

        for doc_idx in target_doc_indices:
            # ... (your existing logic for extracting scores for one doc_idx and building df_doc_analysis) ...
            # This part uses:
            # corpus_tfidf_matrix_for_targets[doc_idx]
            # tf_matrix_for_targets[doc_idx]
            # current_global_term_idf_map
            # current_global_term_doc_count_map
            # all_stem_to_raw_maps_list[doc_idx]
            # tfidf_feature_names
            # current_fitted_tf_vectorizer.vocabulary_
            # ... to populate 'Term (Processed)', 'Original Word(s)', 'TF (in doc)', 'IDF (global)', 'TF-IDF Score', 'Global Doc Freq'
            # (Your existing code for this part from two responses ago was mostly correct, just ensure it uses these variables)
            if doc_idx >= corpus_tfidf_matrix_for_targets.shape[0]: continue  # Bounds check
            doc_tfidf_vector = corpus_tfidf_matrix_for_targets[doc_idx]
            doc_tf_vector = tf_matrix_for_targets[doc_idx]
            doc_data = []
            current_doc_stem_to_raw_map = all_stem_to_raw_maps_list[doc_idx]
            for col_idx in doc_tfidf_vector.nonzero()[1]:
                term = tfidf_feature_names[col_idx]
                tfidf_score_val = doc_tfidf_vector[0, col_idx]
                tf_term_idx = current_fitted_tf_vectorizer.vocabulary_.get(term)
                tf_score_val = doc_tf_vector[0, tf_term_idx] if tf_term_idx is not None and tf_term_idx < \
                                                                doc_tf_vector.shape[1] else 0
                idf_score_val = current_global_term_idf_map.get(term, 0.0)
                global_doc_freq_val = current_global_term_doc_count_map.get(term, 0)
                original_words_val = format_original_words(current_doc_stem_to_raw_map.get(term, {term}))
                doc_data.append({
                    'Term (Processed)': term, 'Original Word(s)': original_words_val,
                    'TF (in doc)': tf_score_val, 'IDF (global)': idf_score_val,
                    'TF-IDF Score': tfidf_score_val, 'Global Doc Freq': global_doc_freq_val
                })
            df_doc_analysis = pd.DataFrame(doc_data).sort_values(by='TF-IDF Score', ascending=False).reset_index(
                drop=True) if doc_data else pd.DataFrame()
            results_dfs.append(df_doc_analysis)

    return results_dfs, current_fitted_tf_vectorizer, current_fitted_tfidf_vectorizer, current_global_term_doc_count_map, current_global_term_idf_map

def calculate_qa_level_tfidf_aggregates(  # Copied
        list_of_doc_analysis_dfs, qa_combined_stem_to_raw_map,
        global_term_idf_map, global_term_doc_count_map):
    if not list_of_doc_analysis_dfs: return pd.DataFrame()
    term_sum_tfidf, term_sum_tf, term_doc_count_in_qa, all_terms_in_qa_docs = defaultdict(float), defaultdict(
        float), defaultdict(int), set()
    for df_doc in list_of_doc_analysis_dfs:
        if df_doc.empty or not all(
            c in df_doc.columns for c in ['Term (Processed)', 'TF-IDF Score', 'TF (in doc)']): continue
        unique_terms_this_doc = set()
        for _, r in df_doc.iterrows():
            t, ts, tfs = r['Term (Processed)'], r['TF-IDF Score'], r['TF (in doc)']
            term_sum_tfidf[t] += ts;
            term_sum_tf[t] += tfs
            all_terms_in_qa_docs.add(t);
            unique_terms_this_doc.add(t)
        for t in unique_terms_this_doc: term_doc_count_in_qa[t] += 1
    if not all_terms_in_qa_docs: return pd.DataFrame()
    agg_data = []
    for t in sorted(list(all_terms_in_qa_docs)):
        stfidf, stf, dcqa = term_sum_tfidf[t], term_sum_tf[t], term_doc_count_in_qa[t]
        avgtfidf, avgtf = (stfidf / dcqa if dcqa > 0 else 0), (stf / dcqa if dcqa > 0 else 0)
        idfg, gdf = global_term_idf_map.get(t, 0), global_term_doc_count_map.get(t, 0)
        orig = format_original_words(qa_combined_stem_to_raw_map.get(t, {t}))
        agg_data.append({'Term (Processed)': t, 'Original Word(s)': orig, 'Summed TF-IDF (QA)': stfidf,
                         'Avg TF-IDF (QA docs with term)': avgtfidf, 'Summed TF (QA)': stf,
                         'Avg TF (QA docs with term)': avgtf, 'IDF (global)': idfg,
                         'Global Doc Freq': gdf, 'Doc Count (in QA)': dcqa})
    return pd.DataFrame(agg_data).sort_values(by=['Summed TF-IDF (QA)', 'Avg TF-IDF (QA docs with term)'],
                                              ascending=[False, False]).reset_index(drop=True)


def prepare_seed_keywords_sheet_data(raw_seed_keywords_list):  # Copied
    data = []
    if not raw_seed_keywords_list: return data
    for seed in raw_seed_keywords_list:
        tokens, _ = preprocess_text_and_map(seed.lower())
        proc_seed = " ".join(tokens) if tokens else seed.lower()
        data.append({'Raw Seed Keyword': seed, 'Processed Seed Keyword': proc_seed})
    return data


def extract_ngrams(processed_tokens, stem_to_raw_map, n=2, num_top_ngrams=50):  # Copied
    if not processed_tokens or len(processed_tokens) < n: return pd.DataFrame()
    tuples = list(nltk_ngrams(processed_tokens, n))
    if not tuples: return pd.DataFrame()
    fd = FreqDist(tuples);
    common = fd.most_common(num_top_ngrams)
    proc, orig, frq = [], [], []
    for ng_tuple, f in common:
        proc.append(" ".join(ng_tuple))
        orig.append(" ".join([format_original_words(stem_to_raw_map.get(t, {t})) for t in ng_tuple]))
        frq.append(f)
    return pd.DataFrame({f'{n}-gram (Processed)': proc, f'Original {n}-gram(s)': orig, 'Frequency': frq})


def find_collocations_general(processed_tokens, stem_to_raw_map, num_collocations=50, window_size=5):  # Copied
    if not processed_tokens or len(processed_tokens) < window_size: return pd.DataFrame()
    bm = BigramAssocMeasures()
    try:
        finder = BigramCollocationFinder.from_words(processed_tokens, window_size=window_size)
        scored = finder.score_ngrams(bm.pmi)
    except:
        return pd.DataFrame()
    if not scored: return pd.DataFrame()
    proc, orig, pmi = [], [], []
    for (w1, w2), s in scored:
        if len(proc) >= num_collocations: break
        proc.append(f"{w1} {w2}")
        orig.append(
            f"{format_original_words(stem_to_raw_map.get(w1, {w1}))} {format_original_words(stem_to_raw_map.get(w2, {w2}))}")
        pmi.append(s)
    return pd.DataFrame(
        {'Collocation (Processed)': proc, 'Original Collocation(s)': orig, 'PMI Score': pmi}).sort_values(
        by='PMI Score', ascending=False).reset_index(drop=True).head(num_collocations)


def find_collocations_with_seeds(processed_tokens, stem_to_raw_map, processed_seed_keywords, num_collocations=50,
                                 window_size=5):  # Copied
    if not processed_tokens or not processed_seed_keywords or len(processed_tokens) < window_size: return pd.DataFrame()
    bm = BigramAssocMeasures()
    try:
        finder = BigramCollocationFinder.from_words(processed_tokens, window_size=window_size)
        scored = finder.score_ngrams(bm.pmi)
    except:
        return pd.DataFrame()
    data = []
    for (w1, w2), s in scored:
        if w1 in processed_seed_keywords or w2 in processed_seed_keywords:
            orig_w1 = format_original_words(stem_to_raw_map.get(w1, {w1}))
            orig_w2 = format_original_words(stem_to_raw_map.get(w2, {w2}))
            data.append({'Collocation (Processed)': f"{w1} {w2}", 'Original Collocation(s)': f"{orig_w1} {orig_w2}",
                         'PMI Score': s, 'Contains Seed': True})
    if not data: return pd.DataFrame()
    return pd.DataFrame(data).sort_values(by='PMI Score', ascending=False).reset_index(drop=True).head(num_collocations)


# --- New Function for Refined c-TF-IDF Calculation ---
def calculate_refined_c_tfidf(
        qa_name_current,
        all_qa_data,
        global_term_idf_map,
        global_term_doc_count_map
):
    if qa_name_current not in all_qa_data or not all_qa_data[qa_name_current]['tokens']:
        return pd.DataFrame()

    current_qa_data = all_qa_data[qa_name_current]
    current_qa_total_tokens = current_qa_data['tokens']
    current_qa_stem_to_raw_map = current_qa_data['stem_to_raw']
    num_docs_in_current_qa = current_qa_data['num_docs']

    sum_tf_tc = FreqDist(current_qa_total_tokens)

    term_doc_count_in_qa = defaultdict(int)
    if 'per_doc_token_sets' in current_qa_data and current_qa_data['per_doc_token_sets']:
        for doc_token_set in current_qa_data['per_doc_token_sets']:
            for term_in_doc in doc_token_set:
                if term_in_doc in sum_tf_tc:
                    term_doc_count_in_qa[term_in_doc] += 1
    else:
        for term in sum_tf_tc.keys():
            if sum_tf_tc[term] > 0:
                term_doc_count_in_qa[term] = max(1, term_doc_count_in_qa.get(term, 0))

    N_total_classes = len(all_qa_data)
    if N_total_classes == 0: return pd.DataFrame()

    unique_tokens_for_all_qas = {
        qa_n: set(qa_d.get('tokens', []))
        for qa_n, qa_d in all_qa_data.items()
    }

    term_to_class_count_map = defaultdict(int)
    for term_in_current_qa in sum_tf_tc.keys():
        count = 0
        for qa_name_other in all_qa_data.keys():
            if term_in_current_qa in unique_tokens_for_all_qas[qa_name_other]:
                count += 1
        term_to_class_count_map[term_in_current_qa] = count if count > 0 else 1

    output_data = []
    for term, total_tf_in_qa in sum_tf_tc.items():
        doc_count_term_in_qa_val = term_doc_count_in_qa.get(term, 0)

        if total_tf_in_qa > 0 and doc_count_term_in_qa_val == 0:
            doc_count_term_in_qa_val = 1 # Local consistency check for QA data

        avg_tf_in_qa_present_docs = total_tf_in_qa / doc_count_term_in_qa_val if doc_count_term_in_qa_val > 0 else 0.0
        avg_tf_in_qa_all_docs = total_tf_in_qa / num_docs_in_current_qa if num_docs_in_current_qa > 0 else 0.0

        num_classes_containing_term = term_to_class_count_map.get(term, 1)
        class_idf_score = 0.0
        if num_classes_containing_term > 0:
            class_idf_score = math.log(1 + (N_total_classes / num_classes_containing_term))

        c_tfidf_sum_tf = total_tf_in_qa * class_idf_score
        c_tfidf_avg_tf_present = avg_tf_in_qa_present_docs * class_idf_score
        c_tfidf_avg_tf_all_qa_docs = avg_tf_in_qa_all_docs * class_idf_score

        # Get global values directly from the maps.
        # With the fix in get_term_frequencies, these maps should now be accurate
        # and contain all terms from your preprocessing.
        global_idf_val = global_term_idf_map.get(term, 0.0)
        global_doc_freq_val = global_term_doc_count_map.get(term, 0)

        # If after the primary fix, global_doc_freq_val is STILL 0 for a term with total_tf_in_qa > 0,
        # it would indicate a very subtle, remaining issue. However, the primary fix should prevent this.
        # For the PUS factor, a GDF of 0 would cause division by 1 (GDF+1), which is fine.
        # A GDF of 0 implies the term is unique to this QA or extremely rare and not picked up globally,
        # which the PUS factor is designed to handle.
        # If global_doc_freq_val is 0 BUT doc_count_term_in_qa_val > 0, this means the term
        # was found in this QA, but not in the global count. This should now be extremely unlikely.
        # The `+1` in PUS factor denominator handles GDF being 0.

        pus_factor_simple = doc_count_term_in_qa_val / (global_doc_freq_val + 1)

        refined_score_sumtf_pus = c_tfidf_sum_tf * pus_factor_simple
        refined_score_avgtf_present_pus = c_tfidf_avg_tf_present * pus_factor_simple
        refined_score_avgtf_all_qa_pus = c_tfidf_avg_tf_all_qa_docs * pus_factor_simple

        original_words = format_original_words(current_qa_stem_to_raw_map.get(term, {term}))

        output_data.append({
            'Term (Processed)': term,
            'Original Word(s)': original_words,
            'Summed TF (in QA)': total_tf_in_qa,
            'Avg TF (QA docs with term)': avg_tf_in_qa_present_docs,
            'Avg TF (all QA docs)': avg_tf_in_qa_all_docs,
            'Doc Count (in QA)': doc_count_term_in_qa_val,
            'N (Total QAs)': N_total_classes,
            '|Ct| (QAs with Term)': num_classes_containing_term,
            'c-IDF (log(1+N/|Ct|))': class_idf_score,
            'c-TF-IDF (SumTF)': c_tfidf_sum_tf,
            'c-TF-IDF (AvgTF_present)': c_tfidf_avg_tf_present,
            'c-TF-IDF (AvgTF_all_QA_docs)': c_tfidf_avg_tf_all_qa_docs,
            'Global IDF': global_idf_val,
            'Global Doc Freq': global_doc_freq_val, # Now reporting the direct value from the corrected map
            'PUS Factor (DC_QA / (GDF+1))': pus_factor_simple,
            'Refined Score (cTFIDF_Sum * PUS)': refined_score_sumtf_pus,
            'Refined Score (cTFIDF_AvgPresent * PUS)': refined_score_avgtf_present_pus,
            'Refined Score (cTFIDF_AvgAllQA * PUS)': refined_score_avgtf_all_qa_pus
        })

    df_ctfidf = pd.DataFrame(output_data)
    if not df_ctfidf.empty:
        df_ctfidf = df_ctfidf.sort_values(
            by=['Refined Score (cTFIDF_Sum * PUS)',
                'Refined Score (cTFIDF_AvgPresent * PUS)',
                'Refined Score (cTFIDF_AvgAllQA * PUS)'],
            ascending=[False, False, False]
        ).reset_index(drop=True)
    return df_ctfidf
# --- Modified `main()` function ---
def main():
    # ensure_nltk_resources()  # Call this to ensure NLTK data is present
    # from sklearn.feature_extraction.text import CountVectorizer # This should be a global import if used in global scope functions

    BASE_PDF_DIR = "../../../metadata/papers/"  # In your script, this is BASE_DOC_DIR now
    NUM_TOP_BIGRAMS, NUM_TOP_TRIGRAMS = 100, 50
    NUM_COLLOCATIONS_GENERAL, NUM_COLLOCATIONS_SEEDS = 200, 100
    COLLOCATION_WINDOW_SIZE = 5
    EXPAND_SEED_SYNONYMS = False
    FORCE_REPROCESS_ALL, FORCE_REFIT_TFIDF = False, False

    print("Starting CORPUS keyword analysis...")
    qa_to_doc_paths_map = find_documents_by_qa(BASE_PDF_DIR)  # Uses the version that finds .pdf and .txt
    if not qa_to_doc_paths_map: print("No documents found. Exiting."); return

    # --- Phase 0: Cache & Ingestion ---
    all_docs_processed_tokens_list, all_docs_stem_to_raw_maps_list, all_docs_original_paths_list = [], [], []
    doc_metadata_for_tfidf_fitting = []

    all_qa_data_for_ctfidf = defaultdict(lambda: {
        'tokens': [],
        'stem_to_raw': defaultdict(set),
        'num_docs': 0,
        'per_doc_token_sets': []
    })

    print("\n--- Phase 0: Ingesting and Preprocessing Documents (with Caching) ---")
    for qa_name, doc_paths in qa_to_doc_paths_map.items():
        print("\n--- Processing Quality Attribute: {qa_name} ---")
        num_docs_processed_for_this_qa = 0
        for doc_path_str in doc_paths:
            print(f"Processing {doc_path_str}")
            doc_path_obj = Path(doc_path_str)
            current_file_hash = get_file_hash(doc_path_str)

            # Create a base cache key - ensure it's filesystem-friendly
            sanitized_stem = "".join(c if c.isalnum() else "_" for c in doc_path_obj.stem)
            max_stem_len = 50
            cache_file_key_base = f"{sanitized_stem[:max_stem_len]}_{current_file_hash if current_file_hash else 'nohash'}"

            # Path for shelve.open() - DO NOT add .dat or other extensions manually
            shelve_base_path_for_file = Path(CACHE_DIR) / cache_file_key_base  # Used for individual doc cache

            processed_tokens, stem_to_raw_map = None, None
            use_cache = False

            if not FORCE_REPROCESS_ALL:  # No need to check exists() if shelve.open 'r' handles it
                try:
                    with shelve.open(str(shelve_base_path_for_file), flag='r') as db:  # Use base path
                        if db.get("preprocessing_version") == PREPROCESSING_VERSION and \
                                db.get("file_hash") == current_file_hash:
                            # print(f"Using cached data for: {doc_path_obj.name}")
                            processed_tokens = db["processed_tokens"]
                            stem_to_raw_map = db["stem_to_raw_map"]
                            use_cache = True
                        # else: print(f"Cache stale for: {doc_path_obj.name}")
                except Exception:  # Catches errors if shelf doesn't exist or can't be opened in 'r' mode
                    # print(f"Cache not found or read error for {doc_path_obj.name}. Will process.")
                    pass

            if not use_cache:
                # print(f"Processing: {doc_path_obj.name} (QA: {qa_name})")
                raw_text = extract_text_from_document(doc_path_str)  # Uses combined PDF/TXT extractor
                if raw_text:
                    processed_tokens, stem_to_raw_map = preprocess_text_and_map(raw_text, doc_path_str)
                    if current_file_hash:  # Only cache if hash was successful (file existed)
                        try:
                            with shelve.open(str(shelve_base_path_for_file), flag='c') as db:  # Use base path
                                db["processed_tokens"], db["stem_to_raw_map"] = processed_tokens, stem_to_raw_map
                                db["file_hash"], db["original_path"] = current_file_hash, doc_path_str
                                db["preprocessing_version"] = PREPROCESSING_VERSION
                            # print(f"Cached: {doc_path_obj.name}")
                        except Exception as e:
                            print(f"Cache write error for {doc_path_obj.name}: {e}")
                    else:  # File hash failed, probably file deleted during run
                        print(f"Skipping cache write for {doc_path_obj.name} as file hash failed.")
                else:
                    processed_tokens, stem_to_raw_map = [], defaultdict(set)

            if processed_tokens is not None:
                all_docs_processed_tokens_list.append(processed_tokens)
                all_docs_stem_to_raw_maps_list.append(stem_to_raw_map)
                all_docs_original_paths_list.append(doc_path_str)
                global_doc_idx = len(all_docs_original_paths_list) - 1
                doc_metadata_for_tfidf_fitting.append(
                    {'path': doc_path_str, 'qa': qa_name, 'global_idx': global_doc_idx})

                all_qa_data_for_ctfidf[qa_name]['tokens'].extend(processed_tokens)
                for stem, raw_set in stem_to_raw_map.items():
                    all_qa_data_for_ctfidf[qa_name]['stem_to_raw'][stem].update(raw_set)
                if processed_tokens:  # Add set of tokens only if there are any
                    all_qa_data_for_ctfidf[qa_name]['per_doc_token_sets'].append(set(processed_tokens))
                num_docs_processed_for_this_qa += 1

        all_qa_data_for_ctfidf[qa_name]['num_docs'] = num_docs_processed_for_this_qa

    if not all_docs_processed_tokens_list: print("No documents processed. Exiting."); return
    all_processed_docs_joined_texts = [" ".join(tokens) for tokens in all_docs_processed_tokens_list]

    # --- Phase 2: Vectorizer Fitting (Global Scope) ---
    # --- Phase 2: Vectorizer Fitting (Global Scope) ---
    # CONSOLIDATED CACHE FILE for all vectorizer-related objects and maps
    vectorizers_and_maps_cache_path = Path(CACHE_DIR) / "fitted_vectorizers_and_global_maps.shelf"

    corpus_raw_text_hash = hashlib.md5("".join(all_processed_docs_joined_texts).encode('utf-8')).hexdigest()

    fitted_tf_vectorizer = None
    fitted_tfidf_vectorizer = None
    global_term_idf_map = None
    global_term_doc_count_map = None
    cache_valid_and_loaded = False

    if not FORCE_REFIT_TFIDF:
        try:
            # Attempt to open the consolidated cache file in read-only mode
            with shelve.open(str(vectorizers_and_maps_cache_path), flag='r') as db:
                if db.get("preprocessing_version") == PREPROCESSING_VERSION and \
                        db.get("corpus_raw_text_hash") == corpus_raw_text_hash:
                    print("Loading cached vectorizers and global term maps from consolidated cache.")
                    fitted_tf_vectorizer = db.get("tf_vectorizer")
                    fitted_tfidf_vectorizer = db.get("tfidf_vectorizer")
                    global_term_doc_count_map = db.get("global_term_doc_count_map")
                    global_term_idf_map = db.get("global_term_idf_map")

                    # Check if all essential components were loaded successfully
                    if all([fitted_tf_vectorizer, fitted_tfidf_vectorizer,
                            global_term_doc_count_map is not None,
                            global_term_idf_map is not None]):  # Check maps for not None
                        cache_valid_and_loaded = True
                    else:
                        print("Consolidated cache was missing some components. Will re-fit.")
                        # Reset all to ensure full re-computation
                        fitted_tf_vectorizer, fitted_tfidf_vectorizer, global_term_idf_map, global_term_doc_count_map = None, None, None, None
                else:
                    print("Consolidated cache stale (version or hash mismatch). Will re-fit.")
        except Exception as e:
            print(f"Failed to load from consolidated cache ({vectorizers_and_maps_cache_path}): {e}. Will re-fit.")

    if not cache_valid_and_loaded:  # If cache was not loaded or was invalid
        print("Re-fitting vectorizers and re-calculating global maps...")

        # Call the function to fit/get all components.
        # It's designed to fit everything if vectorizers/maps are passed as None.
        _, \
            temp_fitted_tf_vectorizer, \
            temp_fitted_tfidf_vectorizer, \
            temp_global_term_doc_count_map, \
            temp_global_term_idf_map = calculate_corpus_tfidf_with_components(
            all_processed_docs_joined_texts,
            all_docs_stem_to_raw_maps_list,
            [],  # No target docs needed for this fitting phase, so first return (list of DFs) is ignored
            None, None, None, None  # Pass None to force fitting of all components
        )

        # Assign successfully fitted components
        if temp_fitted_tf_vectorizer: fitted_tf_vectorizer = temp_fitted_tf_vectorizer
        if temp_fitted_tfidf_vectorizer: fitted_tfidf_vectorizer = temp_fitted_tfidf_vectorizer
        if temp_global_term_doc_count_map is not None: global_term_doc_count_map = temp_global_term_doc_count_map
        if temp_global_term_idf_map is not None: global_term_idf_map = temp_global_term_idf_map

        # If all components were successfully (re)fitted, cache them
        if all([fitted_tf_vectorizer, fitted_tfidf_vectorizer,
                global_term_doc_count_map is not None, global_term_idf_map is not None]):
            try:
                with shelve.open(str(vectorizers_and_maps_cache_path), flag='c') as db:
                    db["tf_vectorizer"] = fitted_tf_vectorizer
                    db["tfidf_vectorizer"] = fitted_tfidf_vectorizer
                    db["global_term_doc_count_map"] = global_term_doc_count_map
                    db["global_term_idf_map"] = global_term_idf_map
                    db["preprocessing_version"] = PREPROCESSING_VERSION
                    db["corpus_raw_text_hash"] = corpus_raw_text_hash
                print("Cached all vectorizers and global term maps to consolidated cache.")
            except Exception as e:
                print(f"Error caching vectorizers and global maps: {e}")
        else:
            print("Warning: Not all vectorizer/map components were successfully fitted. Cache not updated.")

    # Final check after attempting to load from cache or re-fit
    if not all([fitted_tf_vectorizer, fitted_tfidf_vectorizer,
                global_term_doc_count_map is not None, global_term_idf_map is not None]):
        print("CRITICAL: Failed to fit/load necessary vectorizers or global maps. "
              "TF-IDF and c-TF-IDF analyses might be empty or incorrect.")
        # Depending on desired behavior, you might want to:
        # return # Exit if these are critical for all subsequent steps
        # Or, allow N-gram/Collocation to proceed if they don't depend on these global maps directly.
        # For now, the script will continue, but sheets relying on these might be empty.

    # --- Phase 3: Analysis and Output (Per QA) ---
    for qa_name_key, doc_paths_for_qa in qa_to_doc_paths_map.items():
        print(f"\n--- Processing Quality Attribute: {qa_name_key} ---")
        raw_seed_words_for_qa = SEED_WORDS_RAW_MAP.get(qa_name_key, [])
        df_seed_keywords_qa = pd.DataFrame(prepare_seed_keywords_sheet_data(raw_seed_words_for_qa))
        processed_seeds_for_qa = process_seed_keywords(raw_seed_words_for_qa, expand_synonyms=EXPAND_SEED_SYNONYMS)

        qa_doc_global_indices = [m['global_idx'] for m in doc_metadata_for_tfidf_fitting if m['qa'] == qa_name_key]
        if not qa_doc_global_indices: print(f"No docs for QA: {qa_name_key}. Skipping."); continue

        # Standard TF-IDF per doc
        # Ensure all necessary maps are available before calling
        qa_doc_analysis_dfs = []
        if all([fitted_tf_vectorizer, fitted_tfidf_vectorizer, global_term_doc_count_map, global_term_idf_map]):
            qa_doc_analysis_dfs, _, _, _, _ = calculate_corpus_tfidf_with_components(
                all_processed_docs_joined_texts, all_docs_stem_to_raw_maps_list, qa_doc_global_indices,
                fitted_tfidf_vectorizer, fitted_tf_vectorizer, global_term_idf_map, global_term_doc_count_map
            )

        current_qa_aggregated_stem_map = all_qa_data_for_ctfidf[qa_name_key]['stem_to_raw']
        df_qa_tfidf_aggregates = calculate_qa_level_tfidf_aggregates(
            qa_doc_analysis_dfs, current_qa_aggregated_stem_map,
            global_term_idf_map or defaultdict(float),  # Provide default if map is None
            global_term_doc_count_map or defaultdict(int)  # Provide default if map is None
        )

        df_refined_ctfidf_qa = calculate_refined_c_tfidf(
            qa_name_key,
            all_qa_data_for_ctfidf,
            global_term_idf_map or defaultdict(float),  # Provide default
            global_term_doc_count_map or defaultdict(int)  # Provide default
        )

        if not df_refined_ctfidf_qa.empty and not df_seed_keywords_qa.empty and \
                'Term (Processed)' in df_refined_ctfidf_qa.columns and \
                'Processed Seed Keyword' in df_seed_keywords_qa.columns:

            print(f"Calculating keyword matches for QA: {qa_name_key}")
            term_series_for_matching = df_refined_ctfidf_qa['Term (Processed)']
            seed_series_for_matching = df_seed_keywords_qa['Processed Seed Keyword']

            term_to_seed_matches_df, seed_to_term_lookup_for_qa = find_keyword_matches(
                term_series_for_matching,
                seed_series_for_matching
            )

            # Merge results into df_refined_ctfidf_qa
            if not term_to_seed_matches_df.empty:
                df_refined_ctfidf_qa = pd.merge(
                    df_refined_ctfidf_qa,
                    term_to_seed_matches_df,
                    on='Term (Processed)',
                    how='left'  # Keep all terms from df_refined_ctfidf_qa
                )
                # Fill NaN for terms that had no matches (if any, though find_keyword_matches should cover all)
                df_refined_ctfidf_qa['Matching Seed Count'] = df_refined_ctfidf_qa['Matching Seed Count'].fillna(
                    0).astype(int)
                df_refined_ctfidf_qa['Matching Seeds'] = df_refined_ctfidf_qa['Matching Seeds'].fillna("")

            # Prepare data to merge into df_seed_keywords_qa
            if seed_to_term_lookup_for_qa:
                seed_match_counts = {seed: len(terms.split(", ")) if terms else 0 for seed, terms in
                                     seed_to_term_lookup_for_qa.items()}

                df_seed_keywords_qa['Term Match Count'] = df_seed_keywords_qa['Processed Seed Keyword'].map(
                    seed_match_counts).fillna(0).astype(int)
                df_seed_keywords_qa['Matching Terms'] = df_seed_keywords_qa['Processed Seed Keyword'].map(
                    seed_to_term_lookup_for_qa).fillna("")

        else:
            # If one of the DFs is empty or columns are missing, add empty columns to maintain structure
            if not df_refined_ctfidf_qa.empty:
                df_refined_ctfidf_qa['Matching Seed Count'] = 0
                df_refined_ctfidf_qa['Matching Seeds'] = ""
            if not df_seed_keywords_qa.empty:
                df_seed_keywords_qa['Term Match Count'] = 0
                df_seed_keywords_qa['Matching Terms'] = ""
            print(f"Skipping keyword matching for QA: {qa_name_key} due to missing data or columns.")


        current_qa_tokens_for_ngram_colloc = all_qa_data_for_ctfidf[qa_name_key]['tokens']
        df_bigrams_qa = extract_ngrams(current_qa_tokens_for_ngram_colloc, current_qa_aggregated_stem_map, n=2,
                                       num_top_ngrams=NUM_TOP_BIGRAMS)
        df_trigrams_qa = extract_ngrams(current_qa_tokens_for_ngram_colloc, current_qa_aggregated_stem_map, n=3,
                                        num_top_ngrams=NUM_TOP_TRIGRAMS)
        df_colloc_general_qa = find_collocations_general(current_qa_tokens_for_ngram_colloc,
                                                         current_qa_aggregated_stem_map, NUM_COLLOCATIONS_GENERAL,
                                                         COLLOCATION_WINDOW_SIZE)
        df_colloc_seeds_qa = find_collocations_with_seeds(current_qa_tokens_for_ngram_colloc,
                                                          current_qa_aggregated_stem_map, processed_seeds_for_qa,
                                                          NUM_COLLOCATIONS_SEEDS, COLLOCATION_WINDOW_SIZE)


        safe_qa_name = "".join(c if c.isalnum() else "_" for c in qa_name_key)
        keyword_scoring_dir = "../../../data/keyword_scoring"
        os.makedirs(keyword_scoring_dir, exist_ok=True)
        output_excel_qa_path = f"{keyword_scoring_dir}/keyword_scoring_{safe_qa_name}.xlsx"
        print(f"--- Saving results for {qa_name_key} to {output_excel_qa_path} ---")
        try:
            with pd.ExcelWriter(output_excel_qa_path, engine='openpyxl') as writer:
                if not df_refined_ctfidf_qa.empty: df_refined_ctfidf_qa.to_excel(writer, sheet_name='QA_Refined_cTFIDF',
                                                                                 index=False)
                if not df_qa_tfidf_aggregates.empty: df_qa_tfidf_aggregates.to_excel(writer,
                                                                                     sheet_name='QA_StdTFIDF_Aggregates',
                                                                                     index=False)
                if not df_seed_keywords_qa.empty: df_seed_keywords_qa.to_excel(writer, sheet_name='QA_Seed_Keywords',
                                                                               index=False)

                # Writing individual document analysis sheets
                for i, df_doc_analysis in enumerate(qa_doc_analysis_dfs):  # qa_doc_analysis_dfs is the list of DFs
                    if not df_doc_analysis.empty:
                        original_doc_path_str = all_docs_original_paths_list[qa_doc_global_indices[i]]
                        sheet_name_base = Path(original_doc_path_str).stem
                        # Further sanitize sheet name, ensure it's not too long
                        sheet_name_safe = "".join(c if c.isalnum() else "_" for c in sheet_name_base)[:20]

                        unique_sheet_name = sheet_name_safe
                        _count = 1
                        # Use a prefix to avoid clashes and make it clear what these sheets are
                        sheet_prefix = "DocStdAn_"
                        while f"{sheet_prefix}{unique_sheet_name}" in writer.sheets:
                            unique_sheet_name = f"{sheet_name_safe}_{_count}"
                            _count += 1
                            if len(unique_sheet_name) > (31 - len(sheet_prefix) - 2):  # Max sheet name len approx 31
                                # Fallback if name still too long or clashing
                                unique_sheet_name = f"Doc{qa_doc_global_indices[i]}"
                                if f"{sheet_prefix}{unique_sheet_name}" in writer.sheets:  # Ensure fallback is unique
                                    unique_sheet_name = f"Doc{qa_doc_global_indices[i]}_{_count}"
                                break
                        df_doc_analysis.to_excel(writer, sheet_name=f"{sheet_prefix}{unique_sheet_name}", index=False)

                if not df_bigrams_qa.empty: df_bigrams_qa.to_excel(writer, sheet_name='QA_Top_Bigrams', index=False)
                if not df_trigrams_qa.empty: df_trigrams_qa.to_excel(writer, sheet_name='QA_Top_Trigrams', index=False)
                if not df_colloc_general_qa.empty: df_colloc_general_qa.to_excel(writer, sheet_name='QA_Colloc_General',
                                                                                 index=False)
                if not df_colloc_seeds_qa.empty: df_colloc_seeds_qa.to_excel(writer, sheet_name='QA_Colloc_Seeds',
                                                                             index=False)
            print(f"Successfully saved Excel for {qa_name_key}.")
        except Exception as e:
            print(f"Error saving Excel for {qa_name_key}: {e}")
    print("\nCorpus analysis finished.")

if __name__ == "__main__":
    main()