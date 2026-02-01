from main_agent import build_graph
import json

app = build_graph()
print("\n--- 🧪 Verified Test Run 2: Medium Severity (Agentic Skip) ---")
initial_state = {"alert_id": 999} # Triggers MOCK Medium Severity alert
result = app.invoke(initial_state)

# Explicitly print the Router's decision and the Summary
print("\n" + "="*50)
if 'ai_summary' in result:
    print(f"\n[AI Narrative Summary]\n{result['ai_summary']}")
else:
    print("\nSummary failed or not found in result.")
