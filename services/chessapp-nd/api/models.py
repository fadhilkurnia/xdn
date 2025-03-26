from django.db import models

# Create your models here.
# Room model
class Room(models.Model):
    room_id = models.CharField(max_length=100, unique=True)  # Unique identifier for each room
    game_started = models.BooleanField(default=False)  # Indicates whether the game has started

    def __str__(self):
        return f"Room {self.room_id}"

    def to_dict(self):
        return {
            "room_id": self.room_id,
            "game_started": self.game_started,
        }

# Player model
class Player(models.Model):
    room = models.ForeignKey(Room, related_name='players', on_delete=models.CASCADE)  # Each player belongs to a room
    player_id = models.CharField(max_length=100)  # Unique player identifier
    is_ready = models.BooleanField(default=False)  # Indicates if the player is ready
    color = models.CharField(max_length=10)  # The color of the player's piece
    index = models.IntegerField()  # The index of the player (1 or 2)

    def __str__(self):
        return f"Player {self.player_id} in Room {self.room.room_id}"

    def to_dict(self):
        return {
            "room_id": self.room.room_id,
            "player_id": self.player_id,
            "is_ready": self.is_ready,
            "color": self.color,
            "index": self.index,
        }

# GameState model
class GameState(models.Model):
    room = models.ForeignKey(Room, related_name='game_state', on_delete=models.CASCADE)  # Each game state is associated with a room
    color = models.CharField(max_length=10)  
    flags = models.CharField(max_length=100)        # Flags for the move (e.g., castling, en passant, etc.)
    from_square = models.CharField(max_length=10) 
    piece = models.CharField(max_length=10)
    san = models.CharField(max_length=100)          # The move in standard algebraic notation (e.g., "e4", "Nf3")
    to_square = models.CharField(max_length=10)

    def __str__(self):
        return f"GameState in Room {self.room.room_id} Move: {self.san}"

    def to_dict(self):
        return {
            "color": self.color,
            "flags": self.flags,
            "from_square": self.from_square,
            "piece": self.piece,
            "san": self.san,
            "to_square": self.to_square,
        }
