const usernameDialog = document.querySelector('#usernameDialog');
const usernameInput = document.querySelector('input#usernameInput');
const usernameBtn = document.querySelector('button#usernameBtn');
const createBtn = document.querySelector('button#createBtn');
const joinBtn = document.querySelector('button#joinBtn');
const roomInput = document.querySelector('input#roomInput');
const helloH3 = document.querySelector('#hello');
let roomIdInput;
let usernameDataInput;

window.onload = () => {
    roomInput.value = '';
    usernameInput.value = '';

    const uname = sessionStorage.getItem("username");
    if (!uname) {
        usernameDialog.classList.add('flex');
        usernameDialog.classList.remove('hidden');
    }
    helloH3.textContent = `Hello, ${uname}!`;
}

usernameBtn.addEventListener('click', () => {
    // Input Validation
    if (usernameDataInput === undefined 
        || usernameDataInput === '' 
    ) {
        usernameInput.classList.add('vibrate-3');

        setTimeout(() => {
            usernameInput.classList.remove('vibrate-3');
        }, 500);
        return;
    }

    sessionStorage.setItem("username", usernameDataInput);
    usernameDialog.classList.remove('flex');
    usernameDialog.classList.add('hidden');
    helloH3.textContent = `Hello, ${usernameDataInput}!`;
});

createBtn.addEventListener('click', async () => {
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

    // Create room
    const username = sessionStorage.getItem("username");
    const { roomId }  = await createRoom(roomIdInput, username);
    sessionStorage.setItem("roomId", roomId);
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
    const username = sessionStorage.getItem("username");
    const data = await joinRoom(roomIdInput, username);
    if (!data) return;

    sessionStorage.setItem("roomId", roomIdInput);
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

usernameInput.addEventListener('input', (e) => {
    const input = e.target;

    // Remove non-numeric characters
    input.value = input.value.replace(/[^a-zA-Z0-9]/g, '');

    // Limit to 15 digits
    if (input.value.length > 15)
        input.value = input.value.slice(0, 15);

    usernameDataInput = input.value;
});

usernameInput.addEventListener('keydown', (e) => {
    if (e.key !== "Enter") return;

    // Input Validation
    if (usernameDataInput === undefined 
        || usernameDataInput === '' 
    ) {
        usernameInput.classList.add('vibrate-3');

        setTimeout(() => {
            usernameInput.classList.remove('vibrate-3');
        }, 500);
        return;
    }

    sessionStorage.setItem("username", usernameDataInput);
    usernameDialog.classList.remove('flex');
    usernameDialog.classList.add('hidden');
    helloH3.textContent = `Hello, ${usernameDataInput}!`;
})

async function createRoom(id, username) {
    try {
        const res = await fetch('/api/room/create', {
            method: "POST",
            body: JSON.stringify({
                roomId: id,
                username: username,
            }),
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

async function joinRoom(id, username) {
    try {
        const res = await fetch('/api/room/join', {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                roomId: id,
                username, username
            })
        });

        if (!res.ok) return;

        return await res.json();

    } catch (err) {
        console.error(err);
    }
}
