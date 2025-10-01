import re
from typing import Dict, List

QualityAttributesMap = Dict[str, List[str]]


def strip_qa_from_regex(qa):
    qa_without_regex_symbols = re.sub(r'\\b|.\?', '', qa)
    return qa_without_regex_symbols


def qa_sorter(qa: str):
    # Sorts quality attributes by number of words (Highest first), number of hyphens (Highest first),
    # length (Lowest first), and alphabetically (A to Z)
    qa_without_regex_symbols = strip_qa_from_regex(qa)
    return -qa.count(" "), -qa.count("-"), -len(qa_without_regex_symbols), qa_without_regex_symbols


def transform_quality_attributes(qas: QualityAttributesMap, sorter=qa_sorter, *, keep_regex_notation=True):
    return {qa: sorted(keywords if keep_regex_notation else (strip_qa_from_regex(keyword) for keyword in keywords),
                       key=sorter) for qa, keywords in qas.items()}

 

patterns_raw = {
    "datatransfer": [
        # Reduce request frequency / push over poll
        "reduc network call", "reduc network calls", "reduc api call", "reduc api calls",
        "fewer requests", "minimiz requests", "cut request rate", "reduc refresh rate", "lower refresh rate",
        "increas refresh interval", "increas request interval", "refresh less often", "slow refresh", "slow update",
        "refresh on change", "periodic refresh", "every\ \*\ minutes",
        "replac poll", "avoid poll", "stop poll", "long poll", "push over poll", "push updates",
        "server push", "socket\.io", "server-sent events",

        # Rate limiting / retry
        "rate limit request", "rate limiting", "throttl request", "throttling",
        "debounc fetch", "debounc request", "deduplicat request", "coalesc duplicate requests",
        "exponential backoff", "retry with backoff", "backoff with jitter",
        "dynamic retry delay", "increas retry delay", "retry after", "rate limit",

        # Reduce size / compression
        "compress payload", "gzip", "deflate",
        "minify json", "compact json", "shrink payload", "reduc payload",
        "binary json", "messagepack", "cbor",
        "delta update", "diff sync", "patch update",
        "partial response", "sparse fields", "only necessary fields",
        "optimiz data transfer", "reduce bandwidth", "reduce data rate", "lower bitrate",

        # Offload
        "offload compute", "offload processing", "edge offload", "cloud offload",
        "cdn compute", "server-side render", "ssr", "pre-render"
    ],

    "UI": [
        # Images / resolution
        "lazy load image", "lazy load media", "defer offscreen images", "below the fold",
        "defer render", "defer loading",
        "convert to webp", "convert to avif", 
        "serve responsive images", "responsive images", 
        "use smaller resolution", "lower resolution", "downscale images",
        "optimize images", "compress images",

        # Animations / graphics
        "disable animation", "reduc animation", "remov animation", "limit animation",
        "reduce motion", "limit fps", "lower frame rate",
        "heavy paint", "heavy reflow", "expensive render", "render bottleneck", "gpu heavy",
        "canvas heavy", "webgl heavy", "background video", "disable autoplay", "no autoplay", "stop autoplay",
    ],

    "code_optimization": [
        # G1: Common subexpression elimination
        "avoid recompute", "do not recompute", "store result", "memoize",
        "reuse computed value", "common subexpression", "assign to variable", "temporary variable",

        # G2: Sorting
        "avoid resort", "already sorted", "presorted", "skip sort", "nearly sorted", "partial sort",

        # G3: Loop optimizations
        "loop unrolling", "loop unswitching", "early termination", "break early", "guard clause",
        "short circuit in loop", "hoist invariant", "loop invariant", "move call outside loop",
        "avoid expensive call in loop", "store loop end condition", "reduce loop overhead",

        # G4: Short-circuit logic
        "short circuit", "short-circuit operator", "return early",

        # G5: Approximation / lower precision
        "use approximation", "reduce precision", "lower precision",
        "float32", "float16", "bfloat16", "int8", "quantize", "tolerance", "epsilon", "rounding",

        # G6: Remove unnecessary state
        "remove debug variable", "remove temp variable", "avoid storing computed data",
        "reduce intermediate state", "avoid duplicate state",
    ]
}

                          
patterns = transform_quality_attributes(patterns_raw)
