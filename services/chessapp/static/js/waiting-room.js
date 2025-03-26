const idText = document.querySelector('h2#idText');
const readyBtn = document.querySelector('button#readyBtn');
const readyBtnBg = document.querySelector('#readyBtnBg');
const leaveBtn = document.querySelector('button#leaveBtn');

// Set Room ID
idText.textContent = `Room ID: ${sessionStorage.getItem("roomId")}`;

let roomId;
let username;
let playerIndex;

// Main Function
async function onMount() {
    roomId = sessionStorage.getItem("roomId");
    username = sessionStorage.getItem("username");

    // Get player isReady & index
    const playerData = await getPlayerStatus(roomId, username);
    if (!playerData) return;

    // Highlight Player1 or Player2
    playerIndex = playerData.index;

    // Set Initial Player Text Color
    const playerText = document.querySelector(`#player${playerIndex}Text`);
    playerText.textContent = username;
    editReadyStatus(
        playerText, readyBtn, readyBtnBg, 
        'bg-transparent', 'bg-slate-700', 'Ready'
    );

    getRoomInfo();
}

onMount();
standby();

let isReadyP1 = false;
let isReadyP2 = false;

readyBtn.addEventListener('click', async () => {
    const playerText = document.querySelector(`#player${playerIndex}Text`);
    const isReady = (playerIndex === 1) ? isReadyP1 : isReadyP2;

    // Change isReady status in back-end
    await setPlayerStatus(roomId, username, !isReady);
    
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
});

leaveBtn.addEventListener('click', async () => {
    await leaveRoom(roomId, username); 
    sessionStorage.removeItem("roomId");
    window.location.href = '/';
});


function editReadyStatus(refText, refBtn, refBtnBg, prevBg, nextBg, text) {
    if (refText) refText.classList.remove(prevBg);
    if (refText) refText.classList.add(nextBg);
    if (refBtn && text) refBtn.textContent = text;
    if (refBtnBg) refBtnBg.classList.remove(nextBg);
    if (refBtnBg) refBtnBg.classList.add(prevBg);
}

// { isReady: boolean, player: Number, index: Number } 
async function getPlayerStatus(roomId, username) {
    try {
        const res = await fetch(
            `/api/player/status?${new URLSearchParams({
                roomId: roomId,
                username: username,
            }).toString()}`);

        if (!res.ok) return;

        return await res.json();

    } catch (err) {
        console.error(err);
    }
}

async function setPlayerStatus(roomId, username, isReady) {
    try {
        const res = await fetch('/api/player/status', {
            method: "POST",
            body: JSON.stringify({
                roomId: roomId,
                username: username,
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

async function leaveRoom(roomId, username) {
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

async function standby() {
    console.log("Fetching opponent status...");
    try {
        const res = await fetch(
            `/api/room/standby?${new URLSearchParams({
                roomId: roomId,
                username: username,
            }).toString()}`);

        const data = await res.json();
        if (!data) {
            console.log("Data is empty. Refetching in 10 seconds...");
            setTimeout(standby, 10000);
            return;
        }

        console.log("Standby data:", data);
        data.players.forEach(player => {

            // Update Ready Status
            if (player.index === 1) {
                isReadyP1 = player.isReady;
            }
            
            if (player.index === 2) {
                isReadyP2 = player.isReady;
            }

            const playerText = document.querySelector(`#player${player.index}Text`);
            playerText.textContent = player.username;

            // Update UI
            if (player.isReady) {
                editReadyStatus(
                    playerText, undefined, undefined, 
                    'bg-slate-700', 'bg-green-700', ''
                );
            } else {
                editReadyStatus(
                    playerText, undefined, undefined, 
                    'bg-green-700', 'bg-slate-700', ''
                );
            }
        });

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

async function getRoomInfo() {
    try {
        const res = await fetch(
            `/api/room/info?${new URLSearchParams({
                roomId: roomId,
                username: username,
            }).toString()}`);

        const data = await res.json();
        if (!data) return;

        data.players.forEach(player => {
            // Update Ready Status
            if (player.index === 1) isReadyP1 = player.isReady;
            if (player.index === 2) isReadyP2 = player.isReady;

            const playerText = document.querySelector(`#player${player.index}Text`);
            playerText.textContent = player.username;

            // Update UI
            if (player.isReady) {
                editReadyStatus(
                    playerText, undefined, undefined, 
                    'bg-slate-700', 'bg-green-700', ''
                );
            } else {
                editReadyStatus(
                    playerText, undefined, undefined, 
                    'bg-green-700', 'bg-slate-700', ''
                );
            }
        });
    } catch (err) {
        console.error(err);
    }
}

