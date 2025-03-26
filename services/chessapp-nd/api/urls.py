from django.urls import path
from .views import room
from .views import game
from .views import player
from .views import secret

urlpatterns = [
    path("room/create", room.create_room, name="create_room"),
    path("room/join", room.join_room, name="join_rooms"),
    path("room/view", room.view_rooms, name="view_rooms"),
    path("room/leave", room.leave_room, name="leave_room"),
    path("room/standby", room.standby_room, name="standby_room"),
    path("player/status", player.change_status, name="player_status"),
    path("player/view", player.get_players, name="get_players"),
    path("game/data", game.game_player_data, name="game_player_data"),
    path("game/move", game.make_move, name="make_move"),
    path("game/standby", game.standby_game, name="standby_game"),
    path("game/state", game.game_state, name="game_state"),
    path("secret/clear", secret.clear_data, name="clear_data"),
]
