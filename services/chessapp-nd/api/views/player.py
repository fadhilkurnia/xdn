from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from api.models import Room, Player

# Create your views here.
@api_view(['GET'])
def get_players(request):
    players = Player.objects.all()
    return Response({ "players": [player.to_dict() for player in players] })


@api_view(['POST', 'GET'])
def change_status(request):
    """
    GET - Gets player status (including player 1 or player 2)
    POST - Changes the isReady status of player.

    GET params:
    ?roomId=1234&playerId=player-id-example

    POST Request Body (JSON):
    {
        "roomId": "1234",               # required
        "playerId": "player-id-example" # required
        "newStatus": True
    }
    """
    if request.method == 'GET':
        room_id = request.query_params.get('roomId', None)
        player_id = request.query_params.get('playerId', None)

        # Error Checking
        if not room_id:
            return Response({ "message": "'roomId' query is required" }, status=status.HTTP_400_BAD_REQUEST)

        if not player_id:
            return Response({ "message": "'playerId' query is required" }, status=status.HTTP_400_BAD_REQUEST)

        room = Room.objects.filter(room_id=room_id).first()
        if not room:
            return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

        player = Player.objects.filter(room=room, player_id=player_id).first()
        if not player:
            return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)
        
        # Return player status
        return Response({ "isReady": player.is_ready, "index": player.index })
    else:
        body = request.data
        room_id = body.get("roomId")
        player_id = body.get("playerId")
        new_status = body.get("newStatus")

        # Error Checking
        if not room_id:
            return Response({ "message": "'roomId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

        if not player_id:
            return Response({ "message": "'playerId' field is required" }, status=status.HTTP_400_BAD_REQUEST)

        if new_status is None:
            return Response({ "message": "'newStatus' field is required" }, status=status.HTTP_400_BAD_REQUEST)

        room = Room.objects.filter(room_id=room_id).first()
        if not room:
            return Response({ "message": f"Room {room_id} does not exist" }, status=status.HTTP_400_BAD_REQUEST)

        player = Player.objects.filter(room=room, player_id=player_id).first()
        if not player:
            return Response({ "message": f"Player is not a part of Room {room_id}" }, status=status.HTTP_400_BAD_REQUEST)

        # Change player status
        player.is_ready = new_status
        player.save()
        return Response({ "message": "Player status successfully changed" })
