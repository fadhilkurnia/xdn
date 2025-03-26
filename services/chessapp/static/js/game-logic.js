let roomId;
let username;
let color;
let board;
let gameState = [];
const game = new Chess();
const whiteSquareGrey = '#a9a9a9'
const blackSquareGrey = '#696969'

async function onMount() {
    roomId = sessionStorage.getItem("roomId");
    username = sessionStorage.getItem("username");

    const data = await getPlayerData(roomId, username);
    if (!data) return;

    color = data.color;

    console.log("F:", data);
    const config = {
        draggable: true,
        position: 'start',
        onDragStart: onDragStart,
        onDrop: onDrop,
        onMouseoutSquare: onMouseoutSquare,
        onMouseoverSquare: onMouseoverSquare,
        onSnapEnd: onSnapEnd,
        orientation: data.color
    }
    board = Chessboard('myBoard', config)

    const boardState = await getCurrentBoardState(roomId, username);
    if (!boardState) return

    gameState = boardState;
    boardState.forEach(move => {
        game.move({
            color: move.color,
            flags: move.flags,
            from: move.from_square,
            piece: move.piece,
            san: move.san,
            to: move.to_square,
        });
    });
    board.position(game.fen());

    standby();
}
onMount();

// Highlights Possible Moves on Hover
function removeGreySquares () {
    var squares = document.querySelectorAll('#myBoard .square-55d63');
    squares.forEach(square => {
        square.style.background = '';
    });
}

function greySquare(square) {
    const squareElement = document.querySelector(`#myBoard .square-${square}`);
    
    let background = whiteSquareGrey;
    if (squareElement.classList.contains('black-3c85d')) {
        background = blackSquareGrey;
    }
    
    squareElement.style.background = background;
}

// Animation when trying to drag chess piece
// TODO: Add communication to back-end to determine turn
function onDragStart(source, piece) {
    // Only Drag if Piece Color Matches Player Color
    if (!piece) return false;

    const pieceColor = piece.split('')[0];
    const playerColor = color.split('')[0];
    if (pieceColor !== playerColor) return false;

    // do not pick up pieces if the game is over
    if (game.game_over()) return false;

    // or if it's not that side's turn
    if ((game.turn() === 'w' && piece.search(/^b/) !== -1) ||
        (game.turn() === 'b' && piece.search(/^w/) !== -1)) {
        return false;
    }
}

// Animation when chess piece is dropped
// TODO: Add communication to back-end after dropping
async function onDrop (source, target) {
    removeGreySquares()

    // see if the move is legal
    let mv = {
        from: source,
        to: target,
        promotion: 'q' // NOTE: always promote to a queen for example simplicity
    };
    const move = game.move(mv)

    // illegal move
    if (move === null) 
        return 'snapback'
    else {
        gameState.push(move);
        await addMove(roomId, username, move);
    }
}

function onMouseoverSquare (square, piece) {
    if (!piece) return;

    const pieceColor = piece.split('')[0];
    const playerColor = color.split('')[0];
    if (pieceColor !== playerColor) return;

    // get list of possible moves for this square
    var moves = game.moves({
        square: square,
        verbose: true
    })

    // exit if there are no moves available for this square
    if (moves.length === 0) return

    // highlight the square they moused over
    greySquare(square)

    // highlight the possible squares for this piece
    for (var i = 0; i < moves.length; i++) {
        greySquare(moves[i].to)
    }
}

function onMouseoutSquare (square, piece) {
    removeGreySquares()
}

function onSnapEnd () {
    board.position(game.fen())
}

async function getPlayerData(roomId, username) {
    try {
        const res = await fetch(
            `/api/game/data?${new URLSearchParams({
                roomId: roomId,
                username: username,
            }).toString()}`);

        if (!res.ok) return;

        return await res.json();

    } catch (err) {
        console.error(err);
    }
}

async function addMove(roomId, username, move) {
    console.log("addMove:", move);
    try {
        const res = await fetch('/api/game/move', {
            method: "POST",
            body: JSON.stringify({
                roomId: roomId,
                username: username,
                move: move
            }),
            headers: {
                "Content-Type": "application/json"
            },
        });

        if (!res.ok) return;

        return await res.json();

    } catch (err) {
        console.error(err);
    }
}

async function getCurrentBoardState(roomId, username) {
    try {
        const res = await fetch(
            `/api/game/state?${new URLSearchParams({
                roomId: roomId,
                username: username,
            }).toString()}`);

        if (!res.ok) return;

        const data = await res.json();
        return data["gameState"];

    } catch (err) {
        console.error(err);
    }
}

async function standby() {
    try {
        console.log("Fetching game state...");
        const res = await fetch(
            `/api/game/standby?${new URLSearchParams({
                roomId: roomId,
                username: username,
            }).toString()}`);

        if (!res.ok) return;

        const data = await res.json();
        if (!data.gameState) {
            console.log("Game state is empty. Refetching in 10 seconds...");
            setTimeout(standby, 10000);
            return;
        }

        if (gameState.length < data.gameState.length) {
            const newMove = data.gameState.pop();
            game.move({
                color: newMove.color,
                flags: newMove.flags,
                from: newMove.from_square,
                piece: newMove.piece,
                san: newMove.san,
                to: newMove.to_square,
            });
            board.position(game.fen());
        }

        if (game.game_over()) {
            gameOver();
            return;
        }

        // Retrying
        console.log("Refetching...");
        standby();
    } catch (err) {
        console.error(err);
    }
}

function gameOver() {
    const gameOverDialog = document.querySelector("div#gameOver");
    gameOverDialog.classList.remove("hidden");
    gameOverDialog.classList.add("flex");
}

const leaveBtn = document.querySelector("button#leaveBtn");
const leaveBtn1 = document.querySelector("button#leaveBtn1");
leaveBtn.addEventListener("click", async () => {
    await leaveRoom(roomId, username); 
    sessionStorage.removeItem("roomId");
    window.location.href = '/';
});

leaveBtn1.addEventListener("click", async () => {
    const res = await leaveRoom(roomId, username); 

    if (res) {
        sessionStorage.removeItem("roomId");
        window.location.href = '/';
    }
});

async function leaveRoom(roomId, username) {
    console.log("Leaving room...");
    try {
        const res = await fetch('/api/room/leave', {
            method: "POST",
            body: JSON.stringify({
                roomId: roomId,
                username: username,
            }),
            headers: {
                "Content-Type": "application/json"
            },
        });

        if (!res.ok) return;

        return await res.json();

    } catch (err) {
        console.error(err);
    }
}
