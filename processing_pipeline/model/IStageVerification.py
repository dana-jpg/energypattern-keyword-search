from abc import ABC, abstractproperty, abstractmethod
from pydoc import html
from typing import Literal

import pandas as pd
from pydantic import BaseModel

from cfg.ModelName import ModelName
from constants.abs_paths import AbsDirPath
from processing_pipeline.model.CSVDFHandler import CSVDFHandler
from processing_pipeline.model.IBaseStage import IBaseStage


class IStageVerification(IBaseStage, ABC):
    temperature = 0.0
    model_name = ModelName.DEEPSEEK_8B
    in_dir = AbsDirPath.SAMPLES
    out_dir = AbsDirPath.SAMPLES_VERIFIED
    cache_dir = AbsDirPath.CACHE / "samples"
    DFHandler = CSVDFHandler()

    stage_to_verify: IBaseStage

    @property
    def stage_prefix(self) -> str:
        return self.stage_to_verify.stage_name

    @property
    @abstractmethod
    def source_columns(self) -> list[str]:
        """A list of column names that contain the source text(s) for the AI."""
        pass

    @property
    def prompt_column(self) -> str:
        return self.stage_prefix + "_prompt"

    @property
    @abstractmethod
    def ai_output_columns(self) -> list[str]:
        """
        A list of the AI output column SUFFIXES to be verified.
        (e.g., ['to_eliminate', 'reasoning'] for stage 's0')

        """
        pass


    def to_prompt(self, x: pd.Series) -> str:
        """
        Generates the DYNAMIC user prompt.
        It contains only the specific data for this one evaluation item, formatted as defined
        in the system prompt.
        """
        # 1. Prepare the source text block
        source_text_lines = [f"<{col}>{html.escape(str(x.get(col, 'N/A')))}</{col}>" for col in self.source_columns]
        source_text_str = "\n".join(source_text_lines)

        # 2. Prepare the original prompt string
        original_prompt_str = html.escape(x.get(self.prompt_column, 'N/A'))

        # 3. Prepare the AI output block
        ai_output_lines = []
        for col_suffix in self.ai_output_columns:
            full_col_name = f"{self.stage_prefix}_{col_suffix}"
            value = str(x.get(full_col_name, 'N/A'))
            ai_output_lines.append(f"    <{col_suffix}>{html.escape(value)}</{col_suffix}>")
        ai_output_block_str = "\n".join(ai_output_lines)

        # 4. Assemble the final data block for the user message
        return f"""Now, perform your your audit based on the data below.

<evaluation_data>
    <original_system_prompt>
    {self.stage_to_verify.get_system_prompt()}
    </original_system_prompt>
    <original_prompt>
    {original_prompt_str}
    </original_prompt>

    <source_data>
    {source_text_str}
    </source_data>

    <ai_output_to_verify>
    {ai_output_block_str}
    </ai_output_to_verify>
</evaluation_data>
"""

    @property
    def stage_name(self) -> str:
        return self.stage_to_verify.stage_name + '_v'

    def execute_verification(self):
        self.execute([self.stage_to_verify.stage_name])
        # for quick multipart sample verification
        # self.execute([f"{self.stage_to_verify.stage_name}.part"])
