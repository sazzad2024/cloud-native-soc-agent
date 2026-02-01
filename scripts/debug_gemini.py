import os
import getpass
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    print("GOOGLE_API_KEY not found in .env")
else:
    print(f"Testing Gemini with key: {api_key[:10]}...")
    
    # Try gemini-1.5-flash
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=api_key)
    
    try:
        response = llm.invoke("Say hello")
        print(f"Gemini Response: {response.content}")
    except Exception as e:
        print(f"Gemini-1.5-Flash Error: {e}")
        
        print("\nTrying gemini-pro...")
        llm_pro = ChatGoogleGenerativeAI(model="gemini-pro", google_api_key=api_key)
        try:
            response = llm_pro.invoke("Say hello")
            print(f"Gemini-Pro Response: {response.content}")
        except Exception as e2:
            print(f"Gemini-Pro Error: {e2}")
