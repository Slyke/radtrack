<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { closeInfoTip, registerInfoTip, toggleInfoTip } from '$lib/info-tip-manager';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  let {
    text,
    label
  }: {
    text: string | string[];
    label?: string;
  } = $props();

  let trigger = $state<HTMLButtonElement | null>(null);
  let popup = $state<HTMLElement | null>(null);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const accessibleLabel = $derived(label ?? t('radtrack-common_more_information-label'));
  const visible = $derived($sessionStore.userSettings?.showInfoIcons !== false);

  onMount(() => {
    if (!trigger || !popup) {
      return;
    }

    return registerInfoTip({ trigger, popup });
  });

  $effect(() => {
    if (!visible && popup) {
      closeInfoTip({ popup });
    }
  });
</script>

<span class="info-tip" hidden={!visible}>
  <button
    bind:this={trigger}
    type="button"
    class="info-trigger"
    aria-label={accessibleLabel}
    aria-expanded="false"
    title={accessibleLabel}
    onclick={(event) => {
      event.preventDefault();
      event.stopPropagation();
      if (trigger && popup) {
        toggleInfoTip({ trigger, popup });
      }
    }}
  >i</button>
  <span bind:this={popup} class="info-card" popover="manual" role="note">
    {#each Array.isArray(text) ? text : [text] as paragraph}
      <span class="info-paragraph">{paragraph}</span>
    {/each}
  </span>
</span>

<style>
  .info-tip {
    position: relative;
    display: inline-block;
    flex: 0 0 auto;
    margin: 0;
    color: var(--color-text);
    font-size: 0.78rem;
    font-weight: 500;
    letter-spacing: 0;
    line-height: 1.35;
    text-align: left;
    text-transform: none;
  }

  .info-tip[hidden] {
    display: none;
  }

  .info-trigger {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 1.15rem;
    height: 1.15rem;
    min-height: 0;
    padding: 0;
    overflow: visible;
    border: 1px solid var(--color-start);
    border-radius: 50%;
    background: var(--color-start-fill, var(--color-start-soft));
    color: var(--color-start-ink, var(--color-text));
    box-shadow: none;
    cursor: pointer;
    font-size: 0.72rem;
    font-weight: 800;
    letter-spacing: 0;
    line-height: 1;
    transform: none;
    transition:
      background-color var(--transition),
      border-color var(--transition),
      color var(--transition);
  }

  .info-trigger::before,
  .info-trigger::after {
    content: none;
  }

  .info-trigger:hover,
  .info-trigger:global([aria-expanded='true']) {
    border-color: var(--color-warning);
    background: var(--color-warning-fill, var(--color-warning-soft));
    color: var(--color-warning-ink, var(--color-text));
    box-shadow: none;
    transform: none;
  }

  .info-trigger:active {
    border-color: var(--color-warning);
    background: var(--color-warning-fill, var(--color-warning-soft));
    box-shadow: none;
    transform: none;
  }

  .info-trigger:focus-visible {
    outline: 2px solid var(--color-warning);
    outline-offset: 2px;
    border-color: var(--color-warning);
    box-shadow: none;
  }

  .info-card {
    position: fixed;
    inset: auto;
    z-index: 1;
    width: min(19rem, calc(100vw - 1rem));
    max-height: calc(100dvh - 1rem);
    margin: 0;
    padding: 0.65rem 0.75rem;
    overflow-y: auto;
    border: 1px solid var(--color-warning);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
    box-shadow: var(--shadow-panel, var(--shadow-control-hover));
    color: var(--color-text);
    box-sizing: border-box;
    transform: none;
    white-space: normal;
  }

  .info-paragraph {
    display: block;
  }

  .info-paragraph + .info-paragraph {
    margin-top: 0.55rem;
  }
</style>
