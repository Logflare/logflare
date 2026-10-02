import { Combobox as BaseCombobox } from "@base-ui/react/combobox";
import { useEffect, useRef, useState } from "react";

// Enhances a LiveView-managed <select> to a searchable combobox.
// Combobox changes update the <select> and trigger a change event to the LiveView.

// Update form <select> and trigger change.
const setValue = (select, selection) => {
  const values = new Set(
    (Array.isArray(selection) ? selection : [selection])
      .filter(Boolean)
      .map((option) => option.value),
  );

  Array.from(select.options).forEach((option) => {
    option.selected = values.has(option.value);
  });
  select.dispatchEvent(new Event("change", { bubbles: true }));
};

// Build combobox options from <select>.
const optionsFromSelect = (select) => {
  return {
    options: Array.from(select.options)
      .filter((option) => !option.hidden)
      .map((option) => ({
        disabled: option.disabled || option.parentElement.disabled,
        label: option.label,
        value: option.value,
      })),
    value: select.multiple
      ? Array.from(select.selectedOptions).map((option) => option.value)
      : select.value,
  };
};

export function Combobox({ emptyText, prompt, select }) {
  const { options, value: selectValue } = optionsFromSelect(select);

  return select.multiple ? (
    <MultipleCombobox
      emptyText={emptyText}
      options={options}
      prompt={prompt}
      select={select}
      selectValue={selectValue}
    />
  ) : (
    <SingleCombobox
      emptyText={emptyText}
      options={options}
      prompt={prompt}
      select={select}
      selectValue={selectValue}
    />
  );
}

function SingleCombobox({ emptyText, options, prompt, select, selectValue }) {
  const anchorRef = useRef(null);
  const [selectedValue, setSelectedValue] = useState(selectValue);
  const selected = options.find((option) => option.value === selectedValue) || null;
  const [inputValue, setInputValue] = useState(selected?.label || "");

  useEffect(() => setSelectedValue(selectValue), [selectValue]);
  useEffect(() => setInputValue(selected?.label || ""), [selected?.label, selected?.value]);

  return (
    <BaseCombobox.Root
      disabled={select.disabled}
      items={options}
      inputValue={inputValue}
      value={selected}
      onInputValueChange={setInputValue}
      onValueChange={(option) => {
        setSelectedValue(option?.value ?? "");
        setInputValue(option?.label || "");
        setValue(select, option);
      }}
    >
      <div
        ref={anchorRef}
        className="tw-mt-1 tw-flex tw-min-h-[31px] tw-w-full tw-items-center tw-rounded-[0.2rem] tw-border tw-border-solid tw-border-[#ced4da] tw-bg-white focus-within:tw-border-[#80bdff] focus-within:tw-shadow-[0_0_0_0.2rem_rgba(0,123,255,0.25)]"
      >
        <BaseCombobox.Input
          aria-describedby={select.getAttribute("aria-describedby")}
          aria-label={select.getAttribute("aria-label")}
          aria-labelledby={select.getAttribute("aria-labelledby")}
          className="tw-min-w-0 tw-flex-1 tw-border-0 tw-bg-transparent tw-px-2 tw-py-1 tw-text-[#495057] tw-outline-none"
          onKeyDownCapture={(event) => {
            if (event.key === "Enter" && inputValue && !event.currentTarget.ariaActiveDescendant) {
              event.preventDefault();
            }
          }}
          placeholder={prompt}
        />
        <BaseCombobox.Trigger
          className="tw-group tw-flex tw-self-stretch tw-items-center tw-justify-center tw-border-0 tw-bg-transparent tw-px-[0.6rem] tw-py-0 tw-text-[#495057]"
          aria-label="Open options"
        >
          <i
            className="fas fa-angle-down tw-block tw-text-[1.1rem] tw-transition-transform tw-duration-150 group-data-[popup-open]:tw-rotate-180"
            aria-hidden="true"
          />
        </BaseCombobox.Trigger>
      </div>
      <BaseCombobox.Portal>
        <BaseCombobox.Positioner
          anchor={anchorRef}
          className="tw-z-[1050] tw-w-[var(--anchor-width)]"
          sideOffset={4}
        >
          <BaseCombobox.Popup className="tw-max-h-[min(20rem,var(--available-height))] tw-overflow-y-auto tw-rounded tw-border tw-border-solid tw-border-[#ced4da] tw-bg-white tw-p-1 tw-text-[#495057] tw-shadow-[0_0.5rem_1rem_rgba(0,0,0,0.45)]">
            <BaseCombobox.Empty className="tw-px-2 tw-py-[0.4rem] tw-text-[#6c757d]">
              {emptyText}
            </BaseCombobox.Empty>
            <BaseCombobox.List>
              {(option) => (
                <BaseCombobox.Item
                  key={option.value}
                  value={option}
                  disabled={option.disabled}
                  className="tw-flex tw-cursor-default tw-items-center tw-gap-2 tw-rounded-[0.2rem] tw-px-2 tw-py-[0.4rem] tw-outline-none data-[highlighted]:tw-bg-[#2155a3] data-[highlighted]:tw-text-white"
                >
                  <BaseCombobox.ItemIndicator className="tw-w-3">✓</BaseCombobox.ItemIndicator>
                  {option.label}
                </BaseCombobox.Item>
              )}
            </BaseCombobox.List>
          </BaseCombobox.Popup>
        </BaseCombobox.Positioner>
      </BaseCombobox.Portal>
    </BaseCombobox.Root>
  );
}

function MultipleCombobox({ emptyText, options, prompt, select, selectValue }) {
  const anchorRef = useRef(null);
  const selectValueKey = JSON.stringify(selectValue);
  const [selectedValues, setSelectedValues] = useState(selectValue);
  const [inputValue, setInputValue] = useState("");
  const selected = options.filter((option) => selectedValues.includes(option.value));

  useEffect(() => setSelectedValues(selectValue), [selectValueKey]);

  return (
    <BaseCombobox.Root
      disabled={select.disabled}
      items={options}
      inputValue={inputValue}
      multiple
      value={selected}
      onInputValueChange={setInputValue}
      onValueChange={(selection) => {
        setSelectedValues(selection.map((option) => option.value));
        setInputValue("");
        setValue(select, selection);
      }}
    >
      <div
        ref={anchorRef}
        className="tw-mt-1 tw-flex tw-min-h-[38px] tw-w-full tw-items-center tw-rounded-[0.2rem] tw-border tw-border-solid tw-border-[#ced4da] tw-bg-white focus-within:tw-border-[#80bdff] focus-within:tw-shadow-[0_0_0_0.2rem_rgba(0,123,255,0.25)]"
      >
        <BaseCombobox.Value>
          {(value) => (
            <BaseCombobox.Chips
              aria-label={value.length > 0 ? "Selected options" : undefined}
              className="tw-flex tw-min-w-0 tw-flex-1 tw-flex-wrap tw-items-center tw-gap-1 tw-p-1"
            >
              {value.map((option) => (
                <BaseCombobox.Chip
                  key={option.value}
                  aria-description="Press Backspace or Delete to remove"
                  aria-label={option.label}
                  className="tw-flex tw-items-center tw-gap-1 tw-rounded tw-border tw-border-solid tw-border-[#ced4da] tw-bg-[#f8f9fa] tw-px-2 tw-py-1 tw-text-sm tw-text-[#495057]"
                >
                  {option.label}
                  <BaseCombobox.ChipRemove
                    aria-label={`Remove ${option.label}`}
                    className="tw-border-0 tw-bg-transparent tw-p-0 tw-text-[#6c757d]"
                  >
                    <i className="fas fa-times" aria-hidden="true" />
                  </BaseCombobox.ChipRemove>
                </BaseCombobox.Chip>
              ))}
              <BaseCombobox.Input
                aria-describedby={select.getAttribute("aria-describedby")}
                aria-label={select.getAttribute("aria-label")}
                aria-labelledby={select.getAttribute("aria-labelledby")}
                aria-description={
                  value.length > 0
                    ? `${value.length} selected. From the start of the input, press Left Arrow to focus the selected items`
                    : undefined
                }
                className="tw-min-w-[8rem] tw-flex-1 tw-border-0 tw-bg-transparent tw-px-1 tw-py-1 tw-text-[#495057] tw-outline-none"
                onKeyDownCapture={(event) => {
                  if (event.key === "Enter" && inputValue && !event.currentTarget.ariaActiveDescendant) {
                    event.preventDefault();
                  }
                }}
                placeholder={value.length > 0 ? "" : prompt}
              />
            </BaseCombobox.Chips>
          )}
        </BaseCombobox.Value>
        <BaseCombobox.Trigger
          className="tw-group tw-flex tw-self-stretch tw-items-center tw-justify-center tw-border-0 tw-bg-transparent tw-px-[0.6rem] tw-py-0 tw-text-[#495057]"
          aria-label="Open options"
        >
          <i
            className="fas fa-angle-down tw-block tw-text-[1.1rem] tw-transition-transform tw-duration-150 group-data-[popup-open]:tw-rotate-180"
            aria-hidden="true"
          />
        </BaseCombobox.Trigger>
      </div>
      <BaseCombobox.Portal>
        <BaseCombobox.Positioner
          anchor={anchorRef}
          className="tw-z-[1050] tw-w-[var(--anchor-width)]"
          sideOffset={4}
        >
          <BaseCombobox.Popup className="tw-max-h-[min(20rem,var(--available-height))] tw-overflow-y-auto tw-rounded tw-border tw-border-solid tw-border-[#ced4da] tw-bg-white tw-p-1 tw-text-[#495057] tw-shadow-[0_0.5rem_1rem_rgba(0,0,0,0.45)]">
            <BaseCombobox.Empty className="tw-px-2 tw-py-[0.4rem] tw-text-[#6c757d]">
              {emptyText}
            </BaseCombobox.Empty>
            <BaseCombobox.List>
              {(option) => (
                <BaseCombobox.Item
                  key={option.value}
                  value={option}
                  disabled={option.disabled}
                  className="tw-flex tw-cursor-default tw-items-center tw-gap-2 tw-rounded-[0.2rem] tw-px-2 tw-py-[0.4rem] tw-outline-none data-[highlighted]:tw-bg-[#2155a3] data-[highlighted]:tw-text-white"
                >
                  <BaseCombobox.ItemIndicator className="tw-w-3">✓</BaseCombobox.ItemIndicator>
                  {option.label}
                </BaseCombobox.Item>
              )}
            </BaseCombobox.List>
          </BaseCombobox.Popup>
        </BaseCombobox.Positioner>
      </BaseCombobox.Portal>
    </BaseCombobox.Root>
  );
}
