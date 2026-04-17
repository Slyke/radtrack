import { json } from '@sveltejs/kit';
import { toHealthBody } from '$lib/server/build-info';

export const GET = async () => json(toHealthBody({ service: 'radiacode-wui' }));
