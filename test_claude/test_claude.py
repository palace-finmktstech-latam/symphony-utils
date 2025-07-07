import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import anthropic
import uvicorn

# Initialize FastAPI app
app = FastAPI(title="Claude API Proxy", version="1.0.0")

# Check if API key is loaded
api_key = os.getenv("ANTHROPIC_API_KEY")
if not api_key:
    raise ValueError("ANTHROPIC_API_KEY environment variable not found")

# Initialize Anthropic client
client = anthropic.Anthropic(api_key=api_key)

class QuestionRequest(BaseModel):
    question: str

class QuestionResponse(BaseModel):
    answer: str

@app.post("/ask", response_model=QuestionResponse)
async def ask_claude(request: QuestionRequest):
    """
    Send a question to Claude and return the response
    """
    try:
        # Send request to Claude
        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",  # Using Claude 3.5 Sonnet (latest available)
            max_tokens=1000,
            messages=[
                {
                    "role": "user",
                    "content": request.question
                }
            ]
        )
        
        # Extract the text response
        response_text = message.content[0].text
        
        return QuestionResponse(answer=response_text)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error communicating with Claude: {str(e)}")

@app.get("/")
async def root():
    """
    Health check endpoint
    """
    return {"message": "Claude API Proxy is running"}

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy"}

if __name__ == "__main__":
    # Run the server
    uvicorn.run(app, host="127.0.0.1", port=8000)