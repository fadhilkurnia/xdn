from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from random import choice
from uuid import uuid4
import time

from api.models import Room, Player

ALL_ROOM_IDS = [f"{num:04}" for num in range(1,10000)]
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

    Request Body is empty.
    """
    room_ids = Room.objects.all().values("room_id")
    possible_room_ids = list(filter(lambda id: id not in room_ids, ALL_ROOM_IDS))
    room_id = choice(possible_room_ids)

    room = Room.objects.create(room_id=room_id, game_started=False)
    player = Player.objects.create(
        room=room,
        player_id=str(uuid4()),
        is_ready=False,
        color=choice(ORIENTATIONS),
        index=1
    )

    return Response({ "roomId": room_id, "playerId": player.player_id })

@api_view(['POST'])
def join_room(request):
    """
    Adds player into a room.

    Request Body (JSON):
    {
        "roomId": "1234" # required
    }
    """
    body = request.data
    room_id = body.get("roomId")

    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    players_in_room = Player.objects.filter(room=room)
    if len(players_in_room) >= 2:
        return Response({ "message": "Room is full" }, status=status.HTTP_400_BAD_REQUEST)

    taken_color = players_in_room.first().color
    remaining_color = next((color for color in ORIENTATIONS if color != taken_color), None)
    player = Player.objects.create(
        room=room,
        player_id=str(uuid4()),
        is_ready=False,
        color=remaining_color,
        index=len(players_in_room) + 1
    )

    return Response({ "playerId": player.player_id })

@api_view(['POST'])
def leave_room(request):
    """
    Removes player from a room.

    Request Body (JSON):
    {
        "roomId": "1234",               # required
        "playerId": "player-id-example" # required
    }
    """
    body = request.data
    room_id = body.get("roomId")
    player_id = body.get("playerId")

    # Error Checking
    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not player_id:
        return Response({ "message": "'playerId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    player = Player.objects.filter(room=room, player_id=player_id).first()
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
    ?roomId=1234&playerId=player-id-example

    Return:
    {
        "playerData": { "isReady": true, "index": 1},
        "opponentData": { "isReady": true, "index": 2},
        "gameStarted": room["gameStarted"]
     }
    """
    body = request.data
    room_id = request.query_params.get('roomId', None)
    player_id = request.query_params.get('playerId', None)

    # Error Checking
    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not player_id:
        return Response({ "message": "'playerId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    player = Player.objects.filter(room=room, player_id=player_id).exists()
    if not player:
        return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)


    # Polling Logic For Detecting Changes
    prev_data = list(Player.objects.filter(room=room))

    start_time = time.time()
    while time.time() - start_time < 10:  # Long poll for 10 seconds
        # If player status changes
        change = False
        new_data = Player.objects.filter(room=room)
        player_ids = new_data.values_list("player_id", flat=True)

        for id in player_ids:
            prev_status = next((p.is_ready for p in prev_data if p.player_id == id), None)
            new_status = new_data.filter(player_id=id).first().is_ready

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

    player = Player.objects.filter(room=room, player_id=player_id).first()
    opponent = Player.objects.filter(room=room).exclude(player_id=player_id).first()

    return Response({ 
        "playerData": { 
            "isReady": False if not player else player.is_ready, 
            "index": 0 if not player else player.index
        },
        "opponentData": { 
             "isReady": False if not opponent else opponent.is_ready, 
             "index": 0 if not opponent else opponent.index
        },
        "gameStarted": room.game_started
    })



