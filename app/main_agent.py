from typing import TypedDict, List, Dict, Any, Optional
import os
import sys

# Ensure neighbor imports work regardless of execution context
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from connectors import AegisConnector
from rag_engine import RAGEngine
from langchain_google_genai import ChatGoogleGenerativeAI

# 1.# Define paths (Relative to project root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
# 1. Define Agent State
class AgentState(TypedDict):
    alert_id: int
    alert_details: Dict[str, Any]
    user_context: Optional[Dict[str, Any]]
    threat_intel: List[Dict[str, Any]]
    final_report: str
    ai_summary: str
    needs_escalation: bool

# 2. Define Nodes
class SOCWorkflow:
    def __init__(self):
        self.connector = AegisConnector()
        self.rag = RAGEngine()
        # Initialize Gemini LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=os.getenv("GOOGLE_API_KEY")
        )
        # Initialize Cloud Connections
        self.connector.connect_snowflake()
        self.connector.connect_databricks()

    def alert_scanner(self, state: AgentState) -> AgentState:
        """Pulls an alert by ID from state, or latest 'High' severity alert."""
        print("\n[Node] 🚨 Alert Scanner")
        
        target_id = state.get("alert_id")
        
        # MOCK FOR DEMO if Snowflake is unreachable
        if target_id == 999:
            alert_data = {"id": 999, "src_ip": "1.2.3.4", "attack_cat": "DDoS", "severity": "Medium", "user_id": 999}
            print(f"   -> [MOCK] Retrieved SIEM Alert #{alert_data['id']}: {alert_data['attack_cat']}")
            return {"alert_details": alert_data}

        if target_id:
            alerts = self.connector.get_alert_by_id(target_id)
        else:
            alerts = self.connector.get_recent_alerts(limit=1)
        
        if not alerts:
             print(f"   -> ⚠️ Alert #{target_id} not found.") if target_id else print("   -> ⚠️ No active alerts found.")
             # Return a failure state instead of just None
             return {"alert_details": {"error": "Not Found"}}

        latest = alerts[0]
        alert_data = {
            "id": latest[0],
            "src_ip": latest[1],      
            "attack_cat": latest[4],
            "severity": latest[5],
            "user_id": latest[6]
        }
        
        print(f"   -> Retrieved SIEM Alert #{alert_data['id']}: {alert_data['attack_cat']} ({alert_data['severity']})")
        return {"alert_details": alert_data}

    def investigation_router(self, state: AgentState) -> str:
        """Agentic Router: Uses Gemini to decide investigation depth."""
        if state["alert_details"].get("error"):
            return END
            
        print("\n[Agent Logic] 🧠 Consulting Gemini for Investigation Strategy...")
        
        prompt = f"""
        You are a Senior SOC Architect. Review this alert and decide if we need 'Deep Enrichment' (User info + Traffic stats) or 'Fast Track' (Just MITRE mapping).
        
        ALERT: {json.dumps(state['alert_details'])}
        
        Criteria:
        - High Severity or complex attacks (Exploits, Malware) -> 'enrich'
        - Medium/Low or simple attacks (DDoS, Brute Force) -> 'skip'
        
        Response ONLY with the word 'enrich' or 'skip'.
        """
        
        try:
            response = self.llm.invoke(prompt)
            decision = response.content.strip().lower()
        except Exception as e:
            print(f"   -> AI Router Error: {e}. Falling back to rule-based.")
            decision = "enrich" if state["alert_details"].get("severity") == "High" else "skip"
        
        if "enrich" in decision:
            print("   -> AI Action: 🚀 Gemini recommends Full Context Enrichment.")
            return "context_enricher"
        else:
            print("   -> AI Action: ⏩ Gemini recommends Skipping to Threat Analysis.")
            return "threat_analyst"

    def context_enricher(self, state: AgentState) -> AgentState:
        """Enriches alert with User Context from Snowflake and Traffic from Databricks."""
        print("\n[Node] 🔍 Context Enricher")
        
        if not state["alert_details"] or state["alert_details"].get("error"):
            return {}

        user_id = state["alert_details"]["user_id"]
        src_ip = state["alert_details"]["src_ip"]
        
        # 1. Fetch User Info (Snowflake)
        user_data = self.connector.fetch_user_context(user_id)
        
        # 2. Fetch Network Stats (Databricks)
        traffic_data = self.connector.get_traffic_volume(src_ip)
        traffic_vol = traffic_data[0][0] if traffic_data else 0
        
        if user_data:
            # Format: (id, email, dept, risk)
            user_ctx = {
                "id": user_data[0], 
                "email": user_data[1], 
                "department": user_data[2], 
                "risk_score": user_data[3],
                "last_24h_traffic": f"{traffic_vol/1024:.2f} KB" # Added context
            }
            print(f"   -> Identified Internal User: {user_ctx['email']} (Dept: {user_ctx['department']})")
            print(f"   -> Databricks Traffic: {user_ctx['last_24h_traffic']}")
            return {"user_context": user_ctx}
        else:
            print(f"   -> ⚠️ Unauthorized/External Traffic Detected (User ID {user_id} not in directory)")
            user_ctx = {
                 "email": "UNKNOWN_OR_EXTERNAL",
                 "department": "EXTERNAL",
                 "risk_score": 100.0, # High risk because they aren't in our DB
                 "last_24h_traffic": f"{traffic_vol/1024:.2f} KB"
            }
            return {"user_context": user_ctx}

    def threat_analyst(self, state: AgentState) -> AgentState:
        """Identifies MITRE Techniques using RAG."""
        print("\n[Node] 🧠 Threat Analyst (RAG)")
        
        attack_cat = state["alert_details"]["attack_cat"]
        # Ask RAG
        query = f"How does the {attack_cat} attack work?"
        hits = self.rag.search_mitre(query, k=1)
        
        intel = []
        for hit in hits:
            print(f"   -> Mapped to MITRE: {hit['name']} ({hit['id']})")
            intel.append(hit)
            
        return {"threat_intel": intel}

    def manual_escalation(self, state: AgentState) -> AgentState:
        """Fallback node for ambiguous cases."""
        print("\n[Node] 👮 Manual Escalation")
        report = f"ESCALATION REQUIRED: Alert {state['alert_id']} linked to unknown user. Immediate investigation needed."
        return {"final_report": report}

    def triage_report(self, state: AgentState) -> AgentState:
        """Generates the final JSON report."""
        print("\n[Node] 📝 Triage Report")
        
        report_data = {
            "status": "Auto-Triaged",
            "alert": state.get("alert_details"),
            "user": state.get("user_context", "SECURE_SKIP: Low Severity"),
            "mitre_mapping": [t['name'] for t in state.get("threat_intel", [])]
        }
        report_json = json.dumps(report_data, indent=2)
        return {"final_report": report_json}

    def ai_summarizer(self, state: AgentState) -> AgentState:
        """The 'Agentic' Narrator: Uses Gemini to generate a professional summary."""
        print("\n[Node] 🤖 Gemini AI Summarizer")
        
        report_json = state.get("final_report", "{}")
        
        prompt = f"""
        You are a Tier-3 SOC Lead. Author a professional, concise executive summary for this incident.
        Focus on the 'Attack Category', the threat intelligence (MITRE), and the user context provided.
        
        DATA: {report_json}
        
        Format:
        EXECUTIVE SUMMARY: ...
        FINDINGS: ... (Bulleted)
        RECOMMENDATION: ...
        """
        
        try:
            response = self.llm.invoke(prompt)
            summary = response.content
        except Exception as e:
            print(f"   -> AI Summarizer Error: {e}. Using template.")
            summary = "Summary generation failed. Review JSON report for details."
            
        print("   -> Generated Generative AI Executive Summary.")
        return {"ai_summary": summary}

# 3. Build Graph
def build_graph():
    workflow = SOCWorkflow()
    builder = StateGraph(AgentState)
    
    # Add Nodes
    builder.add_node("alert_scanner", workflow.alert_scanner)
    builder.add_node("context_enricher", workflow.context_enricher)
    builder.add_node("threat_analyst", workflow.threat_analyst)
    builder.add_node("triage_report", workflow.triage_report)
    builder.add_node("ai_summarizer", workflow.ai_summarizer)
    builder.add_node("manual_escalation", workflow.manual_escalation)
    
    # Set Entry
    builder.set_entry_point("alert_scanner")
    
    # Edges
    # AGENTIC ROUTING based on Severity: alert_scanner -> (router) -> either context_enricher OR threat_analyst
    builder.add_conditional_edges(
        "alert_scanner",
        workflow.investigation_router,
        {
            "context_enricher": "context_enricher",
            "threat_analyst": "threat_analyst",
            END: END
        }
    )
    
    builder.add_edge("context_enricher", "threat_analyst")
    builder.add_edge("threat_analyst", "triage_report")
    builder.add_edge("triage_report", "ai_summarizer")
    builder.add_edge("ai_summarizer", END)
    
    return builder.compile()

if __name__ == "__main__":
    app = build_graph()
    
    # We'll use IDs that are likely to exist or our mock ID 999
    
    print("--- 🧪 Test Run 1: High Severity (Full Flow) ---")
    initial_state = {} 
    result = app.invoke(initial_state)
    if 'ai_summary' in result:
        print(f"\n[AI Narrative Summary]{result['ai_summary']}")
    
    print("\n" + "="*50)
    print("--- 🧪 Test Run 2: Medium Severity (Agentic Skip) ---")
    initial_state = {"alert_id": 999} # Triggers MOCK Medium Severity alert
    result = app.invoke(initial_state)
    if 'ai_summary' in result:
        print(f"\n[AI Narrative Summary]{result['ai_summary']}")
