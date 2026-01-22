from typing import TypedDict, List, Dict, Any, Optional
import json
from langgraph.graph import StateGraph, END
from connectors import AegisConnector
from rag_engine import RAGEngine

# 1. Define Agent State
class AgentState(TypedDict):
    alert_id: int
    alert_details: Dict[str, Any]
    user_context: Optional[Dict[str, Any]]
    threat_intel: List[Dict[str, Any]]
    final_report: str
    needs_escalation: bool

# 2. Define Nodes
class SOCWorkflow:
    def __init__(self):
        self.connector = AegisConnector()
        self.rag = RAGEngine()
        # Initialize Cloud Connections
        self.connector.connect_snowflake()
        self.connector.connect_databricks()

    def alert_scanner(self, state: AgentState) -> AgentState:
        """Pulls the latest 'High' severity alert (or mock alert for demo)."""
        print("\n[Node] 🚨 Alert Scanner")
        
        # For the demo, we'll manually construct an alert or fetch a real one.
        # REAL PRODUCTION LOGIC:
        # Fetch the latest alert directly from Snowflake (SIEM)
        alerts = self.connector.get_recent_alerts(limit=1)
        
        if not alerts:
             print("   -> ⚠️ No active alerts found in Snowflake.")
             return {"alert_details": None}

        # Snowflake returns a tuple, we map it to our format
        # Structure: (alert_id, src_ip, dst_port, protocol, attack_cat, severity, user_id, flow_duration, total_bytes, timestamp)
        latest = alerts[0]
        
        alert_data = {
            "id": latest[0],
            "src_ip": latest[1],      
            "attack_cat": latest[4],  # Index 4 = attack_cat
            "severity": latest[5],    # Index 5 = severity
            "user_id": latest[6]      # Index 6 = user_id
        }
        
        print(f"   -> Retrieved SIEM Alert #{alert_data['id']}: {alert_data['attack_cat']} from {alert_data['src_ip']}")
        return {"alert_details": alert_data}

    def context_enricher(self, state: AgentState) -> AgentState:
        """Enriches alert with User Context from Snowflake and Traffic from Databricks."""
        print("\n[Node] 🔍 Context Enricher")
        
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
            print(f"   -> Identified User: {user_ctx['email']} (Dept: {user_ctx['department']})")
            print(f"   -> Databricks Traffic: {user_ctx['last_24h_traffic']}")
            return {"user_context": user_ctx, "needs_escalation": False}
        else:
            print(f"   -> ⚠️ No user found for ID {user_id}")
            return {"user_context": None, "needs_escalation": True}

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
            "alert": state["alert_details"],
            "user": state["user_context"],
            "mitre_mapping": [t['name'] for t in state["threat_intel"]]
        }
        report_json = json.dumps(report_data, indent=2)
        return {"final_report": report_json}

# 3. Build Graph
def build_graph():
    workflow = SOCWorkflow()
    builder = StateGraph(AgentState)
    
    # Add Nodes
    builder.add_node("alert_scanner", workflow.alert_scanner)
    builder.add_node("context_enricher", workflow.context_enricher)
    builder.add_node("threat_analyst", workflow.threat_analyst)
    builder.add_node("triage_report", workflow.triage_report)
    builder.add_node("manual_escalation", workflow.manual_escalation)
    
    # Set Entry
    builder.set_entry_point("alert_scanner")
    
    # Edges
    builder.add_edge("alert_scanner", "context_enricher")
    
    # Conditional Edge
    def check_context(state):
        return "manual_escalation" if state.get("needs_escalation") else "threat_analyst"
        
    builder.add_conditional_edges(
        "context_enricher",
        check_context
    )
    
    builder.add_edge("threat_analyst", "triage_report")
    builder.add_edge("triage_report", END)
    builder.add_edge("manual_escalation", END)
    
    return builder.compile()

if __name__ == "__main__":
    app = build_graph()
    
    print("--- 🧪 Test Run 1: Normal Flow ---")
    initial_state = {"alert_id": 1} # Matches User ID 1 (Exists)
    result = app.invoke(initial_state)
    print(f"\n[Final Report]\n{result['final_report']}")
    
    print("\n\n--- 🧪 Test Run 2: Missing User (Escalation) ---")
    initial_state = {"alert_id": 999} # Matches User ID 999 (Does not exist)
    result = app.invoke(initial_state)
    print(f"\n[Final Report]\n{result['final_report']}")
