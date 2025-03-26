const idText = document.querySelector('h2#idText');
const readyBtn = document.querySelector('button#readyBtn');
const readyBtnBg = document.querySelector('#readyBtnBg');
const leaveBtn = document.querySelector('button#leaveBtn');
const statusText = document.querySelector('#statusText');

// Set Room ID
idText.textContent = `Room ID: ${sessionStorage.getItem("roomId")}`;

let roomId;
let playerId;
let playerIndex;

// Main Function
async function onMount() {
    roomId = sessionStorage.getItem("roomId");
    playerId = sessionStorage.getItem("playerId");

    // Get player isReady & index
    const playerData = await getPlayerStatus(roomId, playerId);
    if (!playerData) return;

    // Highlight Player1 or Player2
    playerIndex = playerData.index;
    statusText.textContent = 'You are not ready.';

    // Set Initial Player Text Color
    const playerText = document.querySelector(`#player${playerIndex}Text`);
    editReadyStatus(
        playerText, readyBtn, readyBtnBg, 
        'bg-transparent', 'bg-slate-700', 'Ready'
    );
}

onMount();
standby();

let isReadyP1 = false;
let isReadyP2 = false;

readyBtn.addEventListener('click', async () => {
    const playerText = document.querySelector(`#player${playerIndex}Text`);
    const isReady = (playerIndex === 1) ? isReadyP1 : isReadyP2;

    // Change isReady status in back-end
    await setPlayerStatus(roomId, playerId, !isReady);
    
    // Change front-end UI
    if (isReady) {
        editReadyStatus(
            playerText, readyBtn, readyBtnBg, 
            'bg-green-700', 'bg-slate-700', 'Ready'
        );
    } else {
        editReadyStatus(
            playerText, readyBtn, readyBtnBg, 
            'bg-slate-700', 'bg-green-700', 'Not Ready'
        );
    }

    // Change isReady status in front-end
    if (playerIndex === 1)
        isReadyP1 = !isReadyP1;
    else if (playerIndex === 2)
        isReadyP2 = !isReadyP2;

    handleStatus(playerIndex);
});

leaveBtn.addEventListener('click', async () => {
    await leaveRoom(roomId, playerId); 
    sessionStorage.removeItem("roomId");
    sessionStorage.removeItem("playerId");
    window.location.href = '/';
});

function handleStatus(playerIndex) {
    const youReady = (playerIndex === 1) ? isReadyP1 : isReadyP2;
    const otherReady = (playerIndex === 1) ? isReadyP2 : isReadyP1;

    if (!youReady) {
        statusText.textContent = 'You are not ready.';
        return;
    }

    if (!otherReady) {
        statusText.textContent = 'Waiting for the other player...';
        return;
    }

    statusText.textContent = 'Preparing game...';
}


function editReadyStatus(refText, refBtn, refBtnBg, prevBg, nextBg, text) {
    if (refText) refText.classList.remove(prevBg);
    if (refText) refText.classList.add(nextBg);
    if (refBtn && text) refBtn.textContent = text;
    if (refBtnBg) refBtnBg.classList.remove(nextBg);
    if (refBtnBg) refBtnBg.classList.add(prevBg);
}

// { isReady: boolean, player: Number, index: Number } 
async function getPlayerStatus(roomId, playerId) {
    try {
        const res = await fetch(
            `/api/player/status?${new URLSearchParams({
                roomId: roomId,
                playerId: playerId,
            }).toString()}`);

        if (!res.ok) return;

        return await res.json();

    } catch (err) {
        console.error(err);
    }
}

async function setPlayerStatus(roomId, playerId, isReady) {
    try {
        const res = await fetch('/api/player/status', {
            method: "POST",
            body: JSON.stringify({
                roomId: roomId,
                playerId: playerId,
                newStatus: isReady
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

async function leaveRoom(roomId, playerId) {
    try {
        const res = await fetch('/api/room/leave', {
            method: "POST",
            body: JSON.stringify({
                roomId: roomId,
                playerId: playerId,
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

async function standby() {
    console.log("Fetching opponent status...");
    try {
        const res = await fetch(
            `/api/room/standby?${new URLSearchParams({
                roomId: roomId,
                playerId: playerId,
            }).toString()}`);

        const data = await res.json();
        if (!data.opponentData) {
            console.log("Opponent data is empty. Refetching in 10 seconds...");
            setTimeout(standby, 10000);
            return;
        }

        const opponentIndex = data.opponentData.index;
        const opponentIsReady = data.opponentData.isReady;

        if (opponentIndex === 1) {
            isReadyP1 = opponentIsReady;
        } else if (opponentIndex === 2) {
            isReadyP2 = opponentIsReady;
        }

        // Update Opponent UI
        const opponentText = document.querySelector(`#player${opponentIndex}Text`);
        if (opponentIsReady) {
            editReadyStatus(
                opponentText, undefined, undefined, 
                'bg-transparent', 'bg-green-700', ''
            );
        } else {
            editReadyStatus(
                opponentText, undefined, undefined, 
                'bg-green-700', 'bg-transparent', ''
            );
        }

        handleStatus(data.playerData.index)

        if (data.gameStarted) {
            console.log("Starting game");
            window.location.href = '/game';
        }
    
        // Retrying
        console.log("Refetching...");
        standby();
    } catch (err) {
        console.error(err);
    }
}

