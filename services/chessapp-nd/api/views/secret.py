from rest_framework.decorators import api_view
from rest_framework.response import Response
from api.models import Room, Player, GameState

@api_view(['DELETE'])
def clear_data(request):
    GameState.objects.all().delete()
    Player.objects.all().delete()
    Room.objects.all().delete()
    return Response({ "message": "Data successfully cleared." })
