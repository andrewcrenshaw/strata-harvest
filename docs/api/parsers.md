# Parsers

ATS-specific parsers for job listing extraction. Each parser extends `BaseParser` and auto-registers for its provider.

## Base

::: strata_harvest.parsers.base
    options:
      members:
        - BaseParser

## Greenhouse

::: strata_harvest.parsers.greenhouse
    options:
      members:
        - GreenhouseParser

## Lever

::: strata_harvest.parsers.lever
    options:
      members:
        - LeverParser

## Ashby

::: strata_harvest.parsers.ashby
    options:
      members:
        - AshbyParser

## LLM Fallback

::: strata_harvest.parsers.llm_fallback
    options:
      members:
        - LLMFallbackParser
