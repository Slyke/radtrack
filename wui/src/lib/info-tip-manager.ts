type InfoTipElements = {
  trigger: HTMLButtonElement;
  popup: HTMLElement;
};

let activeTip: InfoTipElements | null = null;
let registeredTipCount = 0;

const isPopupOpen = (popup: HTMLElement) => popup.matches(':popover-open');

const hideTip = ({ trigger, popup }: InfoTipElements, focusTrigger = false) => {
  if (isPopupOpen(popup)) {
    popup.hidePopover();
  }
  trigger.setAttribute('aria-expanded', 'false');
  popup.style.removeProperty('left');
  popup.style.removeProperty('top');
  popup.style.removeProperty('visibility');
  if (activeTip?.popup === popup) {
    activeTip = null;
  }
  if (focusTrigger) {
    trigger.focus();
  }
};

const positionTip = ({ trigger, popup }: InfoTipElements) => {
  const edgeGap = 8;
  const triggerGap = 7;
  const triggerRect = trigger.getBoundingClientRect();
  const popupRect = popup.getBoundingClientRect();
  const viewportWidth = document.documentElement.clientWidth;
  const viewportHeight = document.documentElement.clientHeight;
  const centeredLeft = triggerRect.left + (triggerRect.width / 2) - (popupRect.width / 2);
  const left = Math.min(
    Math.max(edgeGap, centeredLeft),
    Math.max(edgeGap, viewportWidth - popupRect.width - edgeGap)
  );
  const belowTop = triggerRect.bottom + triggerGap;
  const aboveTop = triggerRect.top - popupRect.height - triggerGap;
  const top = belowTop + popupRect.height <= viewportHeight - edgeGap
    ? belowTop
    : Math.max(edgeGap, aboveTop);

  popup.style.left = `${Math.round(left)}px`;
  popup.style.top = `${Math.round(top)}px`;
};

const closeWhenOutside = (event: PointerEvent) => {
  if (
    activeTip
    && event.target instanceof Node
    && !activeTip.popup.contains(event.target)
    && !activeTip.trigger.contains(event.target)
  ) {
    hideTip(activeTip);
  }
};

const closeOnEscape = (event: KeyboardEvent) => {
  if (event.key !== 'Escape' || !activeTip) {
    return;
  }

  hideTip(activeTip, true);
};

const repositionActiveTip = () => {
  if (activeTip) {
    positionTip(activeTip);
  }
};

const addDocumentListeners = () => {
  document.addEventListener('pointerdown', closeWhenOutside, { capture: true });
  document.addEventListener('keydown', closeOnEscape);
  window.addEventListener('scroll', repositionActiveTip, { capture: true });
  window.addEventListener('resize', repositionActiveTip);
};

const removeDocumentListeners = () => {
  document.removeEventListener('pointerdown', closeWhenOutside, { capture: true });
  document.removeEventListener('keydown', closeOnEscape);
  window.removeEventListener('scroll', repositionActiveTip, { capture: true });
  window.removeEventListener('resize', repositionActiveTip);
};

export const registerInfoTip = ({ trigger, popup }: InfoTipElements) => {
  registeredTipCount += 1;
  if (registeredTipCount === 1) {
    addDocumentListeners();
  }

  return () => {
    if (activeTip?.popup === popup) {
      hideTip({ trigger, popup });
    }
    registeredTipCount = Math.max(0, registeredTipCount - 1);
    if (registeredTipCount === 0) {
      removeDocumentListeners();
    }
  };
};

export const closeInfoTip = ({ popup }: { popup: HTMLElement }) => {
  if (activeTip?.popup === popup) {
    hideTip(activeTip);
  }
};

export const toggleInfoTip = ({ trigger, popup }: InfoTipElements) => {
  if (activeTip?.popup === popup && isPopupOpen(popup)) {
    hideTip(activeTip);
    return;
  }

  if (activeTip) {
    hideTip(activeTip);
  }

  popup.style.visibility = 'hidden';
  popup.showPopover();
  positionTip({ trigger, popup });
  popup.style.removeProperty('visibility');
  trigger.setAttribute('aria-expanded', 'true');
  activeTip = { trigger, popup };
};
