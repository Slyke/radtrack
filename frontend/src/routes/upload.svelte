<script>
    import { onMount } from 'svelte';
    let file;

    const uploadFile = async () => {
        const formData = new FormData();
        formData.append('file', file);

        const response = await fetch('http://localhost:3000/upload', {
            method: 'POST',
            body: formData
        });

        if (response.ok) {
            alert('File uploaded successfully');
        } else {
            alert('File upload failed');
        }
    };
</script>

<input type="file" bind:this={file} />
<button on:click={uploadFile}>Upload</button>
