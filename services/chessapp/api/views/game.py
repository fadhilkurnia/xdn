from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import time
from api.models import Room, Player, GameState

# Create your views here.
@api_view(['GET'])
def game_player_data(request):
    """
    GET - Gets player data (index, color)

    GET params:
    ?roomId=1234&playerId=player-id-example
    """
    room_id = request.query_params.get('roomId', None)
    username = request.query_params.get('username', None)

    # Error Checking
    if not room_id:
        return Response({ "message": "'roomId' query is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not username:
        return Response({ "message": "'username' query is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    player = Player.objects.filter(room=room, username=username).first()
    if not player:
        return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)

    # Return player data
    return Response({ "color": player.color })


@api_view(['POST'])
def make_move(request):
    """
    POST - Add a move into the game state

    POST Request Body (JSON):
    {
        "roomId": "1234",               # required
        "username": "username-example"  # required
        "move": { color, flags, from, piece, san, to: string }
    }
    """
    body = request.data
    room_id = body.get("roomId")
    username = body.get("username")
    move = body.get("move")

    # Error Checking
    if not room_id:
        return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not username:
        return Response({ "message": "'username' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    if not move:
        return Response({ "message": "'move' field is required" }, status=status.HTTP_400_BAD_REQUEST)

    room = Room.objects.filter(room_id=room_id).first()
    if not room:
        return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

    player = Player.objects.filter(room=room, username=username).first()
    if not player:
        return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)

    # Add Move
    game_state = GameState.objects.create(
        room=room,
        color=move['color'],
        flags=move['flags'],
        from_square=move['from'],
        piece=move['piece'],
        san=move['san'],
        to_square=move['to']
    )
    return Response({ "message": "Game state successfully updates" })

@api_view(['GET'])
def standby_game(request):
    """
    Polling for player to check room game (gameState).

    GET params:
    ?roomId=1234&username=username-example

    Return:
    {
        "gameState": [{ color, flags, from, piece, san, to: string }]
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

    player = Player.objects.filter(room=room, username=username).first()
    if not player:
        return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)


    # Polling Logic For Detecting Changes
    prev_state = list(GameState.objects.filter(room=room))

    start_time = time.time()
    while time.time() - start_time < 10:  # Long poll for 10 seconds
        # If gameState changes
        new_state = GameState.objects.filter(room=room)
        if (len(new_state) > len(prev_state)):
            break

        time.sleep(1)  # Loop every 1 sec to avoid excessive CPU usage

    game_states = GameState.objects.filter(room=room)
    return Response({ 
        "gameState": [state.to_dict() for state in game_states]
    })


@api_view(['GET'])
def game_state(request):
    """
    Get game state.

    GET params:
    ?roomId=1234&username=username-example

    Return:
    {
        "gameState": [{ color, flags, from, piece, san, to: string }]
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

    player = Player.objects.filter(room=room, username=username).first()
    if not player:
        return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)

    game_states = GameState.objects.filter(room=room)
    return Response({ 
        "gameState": [state.to_dict() for state in game_states]
    })

