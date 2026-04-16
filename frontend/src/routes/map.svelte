<script>
    import { onMount } from 'svelte';
    let data = [];
    let date = '';
    let minIntensity = '';
    let maxIntensity = '';

    const fetchData = async () => {
        const params = new URLSearchParams({
            date,
            minIntensity,
            maxIntensity
        });
        const response = await fetch(`http://localhost:3000/data?${params}`);
        data = await response.json();
    };

    onMount(fetchData);
</script>

<input type="date" bind:value={date} />
<input type="number" placeholder="Min Intensity" bind:value={minIntensity} />
<input type="number" placeholder="Max Intensity" bind:value={maxIntensity} />
<button on:click={fetchData}>Filter</button>

<div id="map"></div>

<script>
    import { onMount } from 'svelte';
    import { Loader } from '@googlemaps/js-api-loader';

    onMount(() => {
        const loader = new Loader({
            apiKey: 'YOUR_GOOGLE_MAPS_API_KEY',
            version: 'weekly'
        });

        loader.load().then(() => {
            const map = new google.maps.Map(document.getElementById('map'), {
                center: { lat: 49.1468739, lng: -122.7963263 },
                zoom: 12
            });

            data.forEach(point => {
                new google.maps.Marker({
                    position: { lat: parseFloat(point.latitude), lng: parseFloat(point.longitude) },
                    map,
                    title: `Dose Rate: ${point.doserate}`
                });
            });
        });
    });
</script>

<style>
    #map {
        height: 500px;
        width: 100%;
    }
</style>
