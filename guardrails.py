
from llm_guard import scan_prompt, scan_output
from llm_guard.input_scanners import Secrets, Toxicity, BanTopics
from llm_guard.input_scanners import PromptInjection
from llm_guard.input_scanners.prompt_injection import MatchType


import sys

if len(sys.argv) > 1:
    # Get user input from command line argument
    user_input = " ".join(sys.argv[1:])

    # Initialize Input Scanners
    input_scanners = [Toxicity(), Secrets(), BanTopics(topics=["violence"]), PromptInjection(threshold=0.5, match_type=MatchType.FULL)]

    # Scan User input
    input_clean, input_results_valid, input_results_invalid = scan_prompt(input_scanners, user_input)

    # If all valid
    if all([i for i in input_results_valid.values()]):
        print("SAFE")
    else:
        print("UNSAFE")

# If user input not provided
else:
    print("Please provide input")

