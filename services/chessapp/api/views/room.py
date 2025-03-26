from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from random import choice
from uuid import uuid4
import time

from api.models import Room, Player

ORIENTATIONS = ["black", "white"]

# Create your views here.
@api_view(['GET'])
def view_rooms(request):
    rooms = Room.objects.all()
    return Response({ "rooms": [room.to_dict() for room in rooms] })

@api_view(['POST'])
def create_room(request):
    """
    Creates a new room and adds the player into the room.

    Request Body (JSON):
    {
        "username": "example-username" # required
        "roomId": "1234" # required
    }
    """
    body = request.data
    room_id = body.get("roomId")
    username = body.get("username")

    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not username:
        return Response({ "message": "'username' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).exists()
    if room:
        return Response({ "message": f"Room {room_id} already exists" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.create(room_id=room_id, game_started=False)
    player = Player.objects.create(
        room=room,
        username=username,
        is_ready=False,
        color=ORIENTATIONS[0],
        index=1
    )

    return Response({ "roomId": room_id })

@api_view(['POST'])
def join_room(request):
    """
    Adds player into a room.

    Request Body (JSON):
    {
        "roomId": "1234" # required
        "username": "example-username" # required
    }
    """
    body = request.data
    room_id = body.get("roomId")
    username = body.get("username")

    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not username:
        return Response({ "message": "'username' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    players_in_room = Player.objects.filter(room=room)
    if len(players_in_room) >= 2:
        return Response({ "message": "Room is full" }, status=status.HTTP_400_BAD_REQUEST)

    player_already_joined = Player.objects.filter(room=room, username=username).exists()
    if player_already_joined:
        return Response({ "message": "Player already joined the room" }, status=status.HTTP_400_BAD_REQUEST)

    taken_color = players_in_room.first().color
    remaining_color = next((color for color in ORIENTATIONS if color != taken_color), None)
    player = Player.objects.create(
        room=room,
        username=username,
        is_ready=False,
        color=remaining_color,
        index=len(players_in_room) + 1
    )

    return Response({ "message": "Player successfully joined room" })

@api_view(['POST'])
def leave_room(request):
    """
    Removes player from a room.

    Request Body (JSON):
    {
        "roomId": "1234",               # required
        "username": "example-username"  # required
    }
    """
    body = request.data
    room_id = body.get("roomId")
    username = body.get("username")

    # Error Checking
    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not username:
        return Response({ "message": "'username' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    player = Player.objects.filter(room=room, username=username).first()
    if not player:
        return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)

    # Remove player from room
    player.delete()

    # Remove room if all players left
    players_in_room = Player.objects.filter(room=room)
    if len(players_in_room) <= 0:
        room.delete()

    return Response({ "message": f"Player has left Room {room_id}" })

@api_view(['GET'])
def standby_room(request):
    """
    Polling for player to check room status (player list, player isReady status).
    Response is sent whenever there's a change in data (in the span of 10 seconds)
    or when both players are ready.

    GET params:
    ?roomId=1234&username=username-example

    Return:
    {
        "players": [
            { "username": "player1", "isReady": false },
            { "username": "player2", "isReady": true },
            ],
        "gameStarted": room["gameStarted"]
     }
    """
    body = request.data
    room_id = request.query_params.get('roomId', None)
    username = request.query_params.get('username', None)

    # Error Checking
    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not username:
        return Response({ "message": "'username' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    player = Player.objects.filter(room=room, username=username).exists()
    if not player:
        return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)

    # Polling Logic For Detecting Changes
    prev_data = list(Player.objects.filter(room=room))

    start_time = time.time()
    while time.time() - start_time < 10:  # Long poll for 10 seconds
        # If player status changes
        change = False
        new_data = Player.objects.filter(room=room)
        usernames = new_data.values_list("username", flat=True)

        if len(new_data) != len(prev_data):
            break

        for uname in usernames:
            prev_status = next((p.is_ready for p in prev_data if p.username == uname), None)
            new_status = new_data.filter(username=uname).first().is_ready

            if (prev_status != new_status):
                change = True

        # If both players are ready
        players_status = [p.is_ready for p in new_data]
        if len(players_status) >= 2 and all(players_status):
            room.game_started = True
            break

        if change:
            break

        time.sleep(1)  # Loop every 1 sec to avoid excessive CPU usage

    players = [p.to_dict() for p in Player.objects.filter(room=room)]
    keys = ["username", "isReady", "index"]
    return Response({ 
        "players": [{key: player[key] for key in keys if key in player} for player in players],
        "gameStarted": room.game_started
    })



@api_view(['GET'])
def room_info(request):
    """
    Get room info

    GET params:
    ?roomId=1234&username=username-example

    Return:
    {
        "players": [
            { "username": "player1", "isReady": false },
            { "username": "player2", "isReady": true },
            ],
        "gameStarted": room["gameStarted"]
     }
    """
    body = request.data
    room_id = request.query_params.get('roomId', None)
    username = request.query_params.get('username', None)

    # Error Checking
    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not username:
        return Response({ "message": "'username' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    player = Player.objects.filter(room=room, username=username).exists()
    if not player:
        return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)

    players = [p.to_dict() for p in Player.objects.filter(room=room)]
    keys = ["username", "isReady", "index"]
    return Response({ 
        "players": [{key: player[key] for key in keys if key in player} for player in players],
        "gameStarted": room.game_started
    })
