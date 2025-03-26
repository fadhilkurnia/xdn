const createBtn = document.querySelector('button#createBtn');
const joinBtn = document.querySelector('button#joinBtn');
const roomInput = document.querySelector('input#roomInput');
let roomIdInput;

window.onload = () => {
    roomInput.value = '';
}

createBtn.addEventListener('click', async () => {
    const { roomId, playerId } = await createRoom();
    sessionStorage.setItem("roomId", roomId);
    sessionStorage.setItem("playerId", playerId);
    window.location.href = '/room';
});

joinBtn.addEventListener('click', async () => {
    // Input Validation
    if (roomIdInput === undefined 
        || roomIdInput === '' 
        || roomIdInput.length < 4
    ) {
        roomInput.classList.add('vibrate-3');

        setTimeout(() => {
            roomInput.classList.remove('vibrate-3');
        }, 500);
        return;
    }
    
    // Join Room
    const data = await joinRoom(roomIdInput);
    if (!data) return;

    const { playerId } = data;
    sessionStorage.setItem("roomId", roomIdInput);
    sessionStorage.setItem("playerId", playerId);
    window.location.href = '/room';
});

roomInput.addEventListener('input', (e) => {
    const input = e.target;

    // Remove non-numeric characters
    input.value = input.value.replace(/[^0-9]/g, '');

    // Limit to 4 digits
    if (input.value.length > 4)
        input.value = input.value.slice(0, 4);

    roomIdInput = e.target.value;
});

async function createRoom() {
    try {
        const res = await fetch('/api/room/create', {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
        });

        if (!res.ok) return;

        return await res.json();

    } catch (err) {
        console.error(err);
    }
}

async function joinRoom(id) {
    try {
        const res = await fetch('/api/room/join', {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                roomId: id,
            })
        });

        if (!res.ok) return;

        return await res.json();

    } catch (err) {
        console.error(err);
    }
}
