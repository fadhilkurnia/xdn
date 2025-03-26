// Load Map
let points = {};
points['San Francisco'] = [37.7879, -122.4075]
points['Los Angeles'] = [37.7879, -122.4075]

let map = L.map('map').setView([
    points['San Francisco'][0],
    points['San Francisco'][1]
], 13);

L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);


// Hotels & Recommendations Toggle
let selectionMode = "hotel";
function createSelectionBox() {
    const hotelsBtn = createHotelsButton();
    const recomBtn = createRecommendationButton();

    hotelsBtn.classList.add("active");
    hotelsBtn.disabled = true;

    hotelsBtn.addEventListener("click", () => {
        selectionMode = "hotel";
        hotelsBtn.classList.add("active");
        hotelsBtn.disabled = true;
        recomBtn.classList.remove("active");
        recomBtn.disabled = false;
    });

    recomBtn.addEventListener("click", () => {
        selectionMode = "recom";
        recomBtn.classList.add("active");
        recomBtn.disabled = true;
        hotelsBtn.classList.remove("active");
        hotelsBtn.disabled = false;
    });
}

function createHotelsButton() {
    const button = document.createElement("button");
    button.classList.add("hotels-button");

    const div = document.createElement("div");
    div.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" height="24px" viewBox="0 -960 960 960" width="24px"><path d="M200-200h-40l-26-80H80v-201q0-33 23.5-56t56.5-23v-120q0-33 23.5-56.5T240-760h480q33 0 56.5 23.5T800-680v120q33 0 56.5 23.5T880-480v200h-54l-26 80h-40l-26-80H226l-26 80Zm320-360h200v-120H520v120Zm-280 0h200v-120H240v120Zm-80 200h640v-120H160v120Zm640 0H160h640Z"/></svg>`;
    const p = document.createElement("p");
    p.textContent = "Hotels";

    button.append(div, p);
    const box = document.querySelector("div.selection-box");
    box.appendChild(button);
    return button;
}

function createRecommendationButton() {
    const button = document.createElement("button");
    button.classList.add("recom-button");

    const div = document.createElement("div");
    div.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" height="24px" viewBox="0 -960 960 960" width="24px"><path d="M360-240h220q17 0 31.5-8.5T632-272l84-196q2-5 3-10t1-10v-32q0-17-11.5-28.5T680-560H496l24-136q2-10-1-19t-10-16l-29-29-184 200q-8 8-12 18t-4 22v200q0 33 23.5 56.5T360-240ZM480-80q-83 0-156-31.5T197-197q-54-54-85.5-127T80-480q0-83 31.5-156T197-763q54-54 127-85.5T480-880q83 0 156 31.5T763-763q54 54 85.5 127T880-480q0 83-31.5 156T763-197q-54 54-127 85.5T480-80Zm0-80q134 0 227-93t93-227q0-134-93-227t-227-93q-134 0-227 93t-93 227q0 134 93 227t227 93Zm0-320Z"/></svg>`;
    const p = document.createElement("p");
    p.textContent = "Recommendations";

    button.append(div, p);
    const box = document.querySelector("div.selection-box");
    box.appendChild(button);
    return button;
}

createSelectionBox();
/*----------*/


// Login (username, password) - Set to local storage
function createLoginButton() {
    const button = document.createElement("button");
    button.classList.add("login-button");

    if (!sessionStorage.getItem("username")) {
        button.textContent = "Login";
        button.classList.add("login");
    } else {
        button.textContent = "Logout";
        button.classList.add("logout");
    }

    const box = document.querySelector("div#overlay");
    box.appendChild(button);

    button.addEventListener("click", () => {
        if (!sessionStorage.getItem("username")) {
            createLoginDialog();
        } else {
            sessionStorage.setItem("username", "");
            sessionStorage.setItem("password", "");
            removeLoginButton();
            createLoginButton();
        }
    });

    return button;
}

function removeLoginButton() {
    const button = document.querySelector(".login-button");
    const box = document.querySelector("div#overlay");
    box.removeChild(button);
}

createLoginButton();

function createLoginDialog() {
    const overlay = document.createElement("div");
    overlay.classList.add("login-overlay");

    const div = document.createElement("div");
    div.classList.add("login-dialog");

    const h3 = document.createElement("h3");
    h3.textContent = "Login";

    const userInput = document.createElement("input");
    const userLabel = document.createElement("label");
    userInput.setAttribute("type", "text");
    userInput.setAttribute("id", "username");
    userInput.setAttribute("name", "username");
    userInput.setAttribute("placeholder", "John");
    userLabel.setAttribute("for", "username");
    userLabel.textContent = "Username";

    const passwordInput = document.createElement("input");
    const passwordLabel = document.createElement("label");
    passwordInput.setAttribute("type", "password");
    passwordInput.setAttribute("id", "password");
    passwordInput.setAttribute("name", "password");
    passwordLabel.setAttribute("for", "password");
    passwordLabel.textContent = "Password";

    const btnBox = document.createElement("div");
    btnBox.classList.add("button-box");

    const okBtn = document.createElement("button");
    okBtn.classList.add("confirm-button");
    okBtn.textContent = "Confirm";
    okBtn.addEventListener("click", () => {
        if (!userInput.value || !passwordInput.value)
            return;

        sessionStorage.setItem('username', userInput.value);
        sessionStorage.setItem('password', passwordInput.value);

        console.log(`Username: ${sessionStorage.getItem('username')}`);
        console.log(`Password: ${sessionStorage.getItem('password')}`);
        removeLoginDialog();
        removeLoginButton();
        createLoginButton();
    });

    const cancelBtn = document.createElement("button");
    cancelBtn.classList.add("cancel-button");
    cancelBtn.textContent = "Cancel";
    cancelBtn.addEventListener("click", () => {
        removeLoginDialog();
    });

    btnBox.append(okBtn, cancelBtn);
    div.append(h3, userLabel, userInput, passwordLabel, passwordInput, btnBox);
    overlay.appendChild(div);
    const box = document.querySelector("div#overlay");
    box.appendChild(overlay);
}

function removeLoginDialog() {
    const overlay = document.querySelector(".login-overlay");
    const box = document.querySelector("div#overlay");
    box.removeChild(overlay);
}
/*----------*/


// Hotel, Recom - Marker Info 
function createMarkerInfoBox() {
    const div = document.createElement("div");
    div.classList.add("marker-info-box");

    const h3 = document.createElement("h3");
    h3.textContent = "Marker Info";

    const latInput = document.createElement("input");
    const latLabel = document.createElement("label");
    latInput.setAttribute("type", "text");
    latInput.setAttribute("id", "latitude");
    latInput.setAttribute("name", "latitude");
    latInput.setAttribute("readonly", true);
    latLabel.setAttribute("for", "latitude");
    latLabel.textContent = "Latitude";

    const lonInput = document.createElement("input");
    const lonLabel = document.createElement("label");
    lonInput.setAttribute("type", "text");
    lonInput.setAttribute("id", "longitude");
    lonInput.setAttribute("name", "longitude");
    lonInput.setAttribute("readonly", true);
    lonLabel.setAttribute("for", "longitude");
    lonLabel.textContent = "Longitude";

    div.append(h3, latLabel, latInput, lonLabel, lonInput);
    const box = document.querySelector("div#overlay");
    box.appendChild(div);
    
    return {
        lat: latInput, 
        lon: lonInput
    };
}

let markerPositionArr = createMarkerInfoBox();

map.on('mousemove', function(e) {
    markerPositionArr['lat'].value = e.latlng.lat.toFixed(3);
    markerPositionArr['lon'].value = e.latlng.lng.toFixed(3);
});
/*----------*/

// Hotel, Recom - Reservation Date
function createReservationBox() {
    const div = document.createElement("div");
    div.classList.add("reservation-box");

    const h3 = document.createElement("h3");
    h3.textContent = "Reservation Date";

    const today = new Date();
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    let todayFmt = today.toISOString().split('T')[0];
    let tomorrowFmt = tomorrow.toISOString().split('T')[0];

    const inLabel = document.createElement("label");
    inLabel.setAttribute("for", "checkIn");
    inLabel.textContent = "Check In";

    const inInput = document.createElement("input");
    inInput.setAttribute("type", "date");
    inInput.setAttribute("id", "checkIn");
    inInput.setAttribute("name", "checkIn");
    inInput.setAttribute("max", todayFmt);
    inInput.value = todayFmt;

    const outLabel = document.createElement("label");
    outLabel.setAttribute("for", "checkOut");
    outLabel.textContent = "Check Out";

    const outInput = document.createElement("input");
    outInput.setAttribute("type", "date");
    outInput.setAttribute("id", "checkOut");
    outInput.setAttribute("name", "checkOut");
    outInput.setAttribute("min", todayFmt);
    outInput.value = tomorrowFmt;

    inInput.addEventListener("input", () => {
        const newToday = inInput.value;
        outInput.setAttribute("min", newToday);
    });

    outInput.addEventListener("input", () => {
        const newTomorrow = outInput.value;
        inInput.setAttribute("max", newTomorrow);
    });

    div.append(h3, inLabel, inInput, outLabel, outInput);
    const box = document.querySelector("div#overlay");
    box.appendChild(div);
    
    return {
        in: inInput, 
        out: outInput
    };
}

let reservationDate = createReservationBox();
/*----------*/

// Load GeoJSON and Hotels List
let geojsonLayer = null;
map.on('click', function(e) {
    removeHotelView();

    if (geojsonLayer) {
        geojsonLayer.clearLayers();
        geojsonLayer = null;
    }

    getHotelGeo(
        reservationDate['in'].value,
        reservationDate['out'].value,
        markerPositionArr['lat'].value,
        markerPositionArr['lon'].value,
    );
});

async function getHotelGeo(inDate, outDate, lat, lon) {
    const response = await fetch(`/hotels?inDate=${inDate}&outDate=${outDate}&lat=${lat}&lon=${lon}`);
    const data = await response.json();
    createHotelList(data);
    geojsonLayer = L.geoJson(data, {
        pointToLayer: function (feature, latlng) {
            // console.log(feature.properties.name);
            return L.marker(latlng, {
                riseOnHover: true,
            });
        },
        onEachFeature: onEachFeature,
    }).addTo(map);
}

function onEachFeature(feature, layer) {
    layer.on("click", async () => {
        removeHotelList();
        const anchor = document.querySelector(".hotel-view-anchor");

        // removeHotelView();
        const data = {
            id: feature.id,
            name: feature.properties.name,
            phone: feature.properties.phone_number,
            lat: feature.geometry.coordinates[0],
            lon: feature.geometry.coordinates[1],
        }
        await createHotelView(data);
    });
}

/*
async function getRecomGeo(lat, lon) {
    const response = await fetch(`/recommendations?require=dis&lat=${lat}&lon=${lon}`);
    const data = await response.json();
    geojsonLayer = L.geoJson(data).addTo(map);
}
*/

function createHotelList(data) {
    const box = document.querySelector("div#overlay");
    let hotelList = data.features.map(item => {
        return {
            id: item.id,
            name: item.properties.name,
            phone: item.properties.phone_number,
            lat: item.geometry.coordinates[0],
            lon: item.geometry.coordinates[1],
        }
    }).sort((a, b) => a.name.localeCompare(b.name));
    

    let anchor = document.querySelector(".hotel-list-anchor");

    if (hotelList.length === 0) {
        if (anchor) box.removeChild(anchor);

        return;
    }

    if (!anchor) {
        anchor = document.createElement("div");
        anchor.classList.add("hotel-list-anchor", "slide-in-left");
    } else {
        while (anchor.firstChild)
            anchor.removeChild(anchor.lastChild);

        anchor.classList.remove("slide-in-left");
    }

    const div = document.createElement("div");
    div.classList.add("hotel-list-box");

    hotelList.forEach(hotel => {
        createHotelRow(div, hotel)
    });

    anchor.appendChild(div)
    box.appendChild(anchor);
    
    return anchor;
}

function createHotelRow(parent, hotel) {
    const div = document.createElement("div");
    div.classList.add("hotel-row");

    const p = document.createElement("p");
    p.classList.add("name");
    p.textContent = hotel.name;

    const id = document.createElement("p");
    id.classList.add("desc");
    id.textContent = `ID: ${hotel.id}`;

    div.addEventListener("click", () => {
        removeHotelList();
        createHotelView(hotel);
    });

    div.append(p, id);
    parent.appendChild(div);
}

function removeHotelList() {
    const ref = document.querySelector(".hotel-list-anchor");
    if (!ref) return;

    ref.classList.add("slide-out-left");
    ref.classList.remove("slide-in-left");

    const box = document.querySelector("div#overlay");
    setTimeout(() => box.removeChild(ref), 1000);
}

/*----------*/

/* Hotel, Recom - Hotel */
async function createHotelView(data) {
    let anchor = document.querySelector(".hotel-view-anchor");

    if (!anchor) {
        anchor = document.createElement("div");
        anchor.classList.add("hotel-view-anchor", "slide-in-left");
    } else {
        while (anchor.firstChild)
            anchor.removeChild(anchor.lastChild);
        
        anchor.classList.remove("slide-in-left");
    }

    const div = document.createElement("div");
    div.classList.add("hotel-view");

    const img = document.createElement("img");
    img.setAttribute("src", "/images/placeholder.jpg");
    div.appendChild(img);

    const username = sessionStorage.getItem("username");
    const password = sessionStorage.getItem("password");
    const response = await fetch(`/profile?username=${username}&password=${password}&hotelId=${data.id}`);
    const json = await response.json();

    const h3 = document.createElement("h3");
    h3.textContent = data.name;
    div.appendChild(h3);

    const p = document.createElement("p");
    p.classList.add("desc");
    p.textContent = json.Description;
    div.appendChild(p);

    const divPhone = document.createElement("div");
    divPhone.classList.add("desc-box");
    const iconPhone = document.createElement("div");
    iconPhone.classList.add("icon");
    iconPhone.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" height="24px" viewBox="0 -960 960 960" width="24px" fill="#5f6368"><path d="M798-120q-125 0-247-54.5T329-329Q229-429 174.5-551T120-798q0-18 12-30t30-12h162q14 0 25 9.5t13 22.5l26 140q2 16-1 27t-11 19l-97 98q20 37 47.5 71.5T387-386q31 31 65 57.5t72 48.5l94-94q9-9 23.5-13.5T670-390l138 28q14 4 23 14.5t9 23.5v162q0 18-12 30t-30 12ZM241-600l66-66-17-94h-89q5 41 14 81t26 79Zm358 358q39 17 79.5 27t81.5 13v-88l-94-19-67 67ZM241-600Zm358 358Z"/></svg>`;
    const pPhone = document.createElement("p");
    pPhone.textContent = json.PhoneNumber;
    divPhone.append(iconPhone, pPhone);
    div.appendChild(divPhone);

    const divAddr = document.createElement("div");
    divAddr.classList.add("desc-box");
    const iconAddr = document.createElement("div");
    iconAddr.classList.add("icon");
    iconAddr.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" height="24px" viewBox="0 -960 960 960" width="24px" fill="#5f6368"><path d="M480-480q33 0 56.5-23.5T560-560q0-33-23.5-56.5T480-640q-33 0-56.5 23.5T400-560q0 33 23.5 56.5T480-480Zm0 294q122-112 181-203.5T720-552q0-109-69.5-178.5T480-800q-101 0-170.5 69.5T240-552q0 71 59 162.5T480-186Zm0 106Q319-217 239.5-334.5T160-552q0-150 96.5-239T480-880q127 0 223.5 89T800-552q0 100-79.5 217.5T480-80Zm0-480Z"/></svg>`;
    const pAddr = document.createElement("p");
    const addr = json.Address;
    pAddr.textContent = `${addr.StreetNumber} ${addr.StreetName} ${addr.City} ${addr.State}`;
    divAddr.append(iconAddr, pAddr);
    div.appendChild(divAddr);

    const box = document.querySelector("div#overlay");
    anchor.appendChild(div);
    box.appendChild(anchor);
}

function removeHotelView() {
    const ref = document.querySelector(".hotel-view-anchor");

    if (!ref) return;

    ref.style["z-index"] = 50;
    ref.classList.add("slide-out-left");
    ref.classList.remove("slide-in-left");

    const box = document.querySelector("div#overlay");
    setTimeout(() => box.removeChild(ref), 1000);
}

/*----------*/
