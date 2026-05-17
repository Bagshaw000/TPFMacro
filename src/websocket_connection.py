from typing import Dict, Set
from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id:str):
        await websocket.accept()
        
        # Checks if the user connection is in active connection
        if user_id not in self.active_connections:
            # if no active connection create a new websocket
            self.active_connections[user_id] = set()
        
        # Add the user to list of active connections   
        self.active_connections[user_id].add(websocket)

    def disconnect(self, websocket: WebSocket, user_id:str):
        
        # Check if the user id has an active connection
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            
            # If the user id has no websocket in them remove the connection
            if not self.active_connections[user_id]:  
                del self.active_connections[user_id]
                print(f"❌ {user_id} fully disconnected")
        

    async def send_personal_message(self, message: str,user_id: str ):
        # Check is the user has any active connection
        if user_id in self.active_connections:
            disconnected = set ()
            
            # loop through all connections then check if the websocket is there
            for connection in self.active_connections[user_id]:
                try:
                   
                    await connection.send_json(message)
                except:
                    disconnected.add(connection)
            
            # If any websocket throw an error then disconnect
            for connection in disconnected:
                self.disconnect(user_id=user_id, websocket=connection)   

    async def broadcast(self, message: str):
        
        
        # Loop through all the users connection
        for user_id, connections in self.active_connections.items():
            disconnected_connections = set()
            
            # Loop through all connections then broad cast the message
            for connection in connections:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    print(f"Failed to send to {user_id}: {e}")
                    disconnected_connections.add(connection)
            
            # Clean up broken connections
            for connection in disconnected_connections:
                self.disconnect(user_id, connection)