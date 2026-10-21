"use strict";
const $ = (id) => document.getElementById(id);
const labels = {available:"Свободно",occupied:"Занято",unavailable:"Недоступно",purchase_required:"Fragment",reserved:"Резерв",invalid:"Некорректно",unsupported_suffix:"Суффикс",account_limited:"Лимит аккаунта",error:"Ошибка",unknown:"Неизвестно",pending:"Не проверено",stale:"Устарело"};
const numbers = new Intl.NumberFormat("ru-RU");
let page = 1, total = 0, controller, debounce, toastTimer;
const initial = new URLSearchParams(location.search);
for (const key of ["q", "status", "sort"]) {
  const input = $(key === "q" ? "search" : key);
  if (initial.has(key)) input.value = initial.get(key);
  if (input.tagName === "SELECT" && !input.value) input.selectedIndex = 0;
}
function params() {
  return new URLSearchParams({q:$("search").value,status:$("status").value,suffix:$("suffix").value,sort:$("sort").value,page:String(page),per_page:"50"});
}
function element(tag, className, text) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (text !== undefined) el.textContent = text;
  return el;
}
function ago(value) {
  if (!value) return "Не проверено";
  const seconds = Math.max(0, (Date.now() - new Date(value).getTime()) / 1000);
  if (seconds < 60) return "Только что";
  if (seconds < 3600) return `${Math.floor(seconds/60)} мин. назад`;
  if (seconds < 86400) return `${Math.floor(seconds/3600)} ч назад`;
  return `${Math.floor(seconds/86400)} дн. назад`;
}
function notify(message) {
  $("toast").textContent = message;
  $("toast").hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => $("toast").hidden = true, 3000);
}
async function copyName(name) {
  try {
    await navigator.clipboard.writeText("@" + name);
    notify("@" + name + " скопировано");
  } catch {
    // Clipboard API may be unavailable on a LAN HTTP origin.
    const input = element("textarea");
    input.value = "@" + name;
    document.body.append(input);
    input.select();
    const copied = document.execCommand("copy");
    input.remove();
    notify(copied ? "@" + name + " скопировано" : "Не удалось скопировать: @" + name);
  }
}
function renderRows(items) {
  const fragment = document.createDocumentFragment();
  for (const item of items) {
    const row = element("tr");
    const nameCell = element("td");
    const link = element("a", "username", "@" + item.username);
    link.href = "https://t.me/" + encodeURIComponent(item.username);
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    nameCell.append(link, element("br"), element("span", "suffix-tag", "+ " + item.suffix));
    const companyCell = element("td");
    const company = element("div", "company", item.company);
    company.title = item.company + " · " + item.source;
    companyCell.append(company);
    const statusCell = element("td");
    const badge = element("span", "badge " + item.status, labels[item.status] || item.status);
    badge.title = item.status === "stale" ? `Раньше: ${labels[item.previous_status] || item.previous_status}. ${item.detail}` : item.detail;
    statusCell.append(badge);
    const checkedCell = element("td", "checked", ago(item.checked_at_utc));
    checkedCell.title = item.checked_at_utc ? new Date(item.checked_at_utc).toLocaleString("ru-RU") : "Проверка ещё не выполнялась";
    const actionCell = element("td");
    const button = element("button", "copy-button", "⧉");
    button.type = "button";
    button.title = "Копировать @" + item.username;
    button.setAttribute("aria-label", button.title);
    button.addEventListener("click", () => copyName(item.username));
    actionCell.append(button);
    row.append(nameCell, companyCell, statusCell, checkedCell, actionCell);
    fragment.append(row);
  }
  $("names").replaceChildren(fragment);
}
function renderWorker(data) {
  const worker = data.worker;
  let status = worker?.status || "stopped";
  let description = "Запустите worker, чтобы начать циклические проверки.";
  const states = {running:"● Проверка идёт",connecting:"◌ Подключение к Telegram",waiting:"◷ Ожидание следующей проверки",flood_wait:"◷ Пауза Telegram",error:"! Повтор после ошибки",stopped:"○ Обработчик остановлен",offline:"○ Нет связи с обработчиком"};
  if (worker && data.blocked_until > Date.now()/1000 && !["stopped", "offline"].includes(status)) {
    status = "flood_wait";
    description = "Telegram запросил паузу до " + new Date(data.blocked_until * 1000).toLocaleString("ru-RU") + ". Очередь сохранена.";
  } else if (status === "running") {
    description = worker.username ? "Текущее имя: @" + worker.username + ". После ответа — следующее по очереди." : "Сначала непроверенные имена, затем самые старые проверки.";
  } else if (["error", "waiting", "flood_wait"].includes(status)) {
    description = (worker.message || "Пауза между проверками.") + (worker.resume_at ? " Продолжение: " + new Date(worker.resume_at*1000).toLocaleString("ru-RU") + "." : "");
  } else if (status === "offline") {
    description = "Обработчик давно не обновлял состояние. Проверьте, запущен ли сервис worker.";
  } else if (status === "connecting") {
    description = "Подключаемся с сохранённой Telegram-сессией.";
  }
  $("worker-badge").className = "worker-badge " + status;
  $("worker-badge").textContent = states[status] || status;
  $("worker-description").textContent = description;
  $("worker-updated").textContent = worker ? ago(new Date(worker.updated*1000).toISOString()) : "Ещё не запускался";
  const queue = data.queue.map((item) => {
    const li = element("li");
    const name = element("div", "queue-name");
    name.append(element("strong", "", "@" + item.username), element("span", "", ago(item.checked_at_utc)));
    li.append(name, element("span", "queue-arrow", "↗"));
    return li;
  });
  $("queue").replaceChildren(...queue);
  if (!queue.length) $("queue").append(element("li", "queue-note", "Каталог ещё пуст"));
}
function render(data) {
  total = data.total;
  const stats = data.stats;
  $("stat-available").textContent = numbers.format(stats.available || 0);
  $("stat-total").textContent = numbers.format(Object.values(stats).reduce((a,b) => a+b, 0));
  $("stat-pending").textContent = numbers.format(stats.pending || 0);
  $("stat-stale").textContent = numbers.format(stats.stale || 0);
  $("result-count").textContent = numbers.format(total);
  $("cache-hours").textContent = data.cache_hours;
  const selected = $("suffix").value || initial.get("suffix") || "";
  $("suffix").replaceChildren(new Option("Все суффиксы", ""), ...data.suffixes.map(s => new Option("+ " + s, s)));
  $("suffix").value = data.suffixes.includes(selected) ? selected : "";
  initial.delete("suffix");
  renderRows(data.items);
  renderWorker(data);
  $("loading").hidden = true;
  $("empty-state").hidden = data.items.length > 0;
  document.querySelector(".table-wrap").hidden = data.items.length === 0;
  const filtered = $("search").value || $("suffix").value || $("status").value !== "available";
  $("empty-title").textContent = filtered ? "Ничего не найдено" : "Свободных имён пока нет";
  $("empty-description").textContent = filtered ? "Попробуйте другое имя, суффикс или статус." : "Здесь появятся имена с положительным ответом Telegram. Устаревшие проверки доступны через фильтр статуса.";
  $("clear-filters").hidden = !filtered;
  $("page-info").textContent = total ? `${numbers.format((page-1)*50+1)}–${numbers.format(Math.min(page*50,total))} из ${numbers.format(total)}` : "0 имён";
  $("page-number").textContent = `${page} / ${Math.max(1,Math.ceil(total/50))}`;
  $("previous").disabled = page === 1;
  $("next").disabled = page*50 >= total;
  $("last-refresh").textContent = "Обновлено " + new Date(data.updated_at).toLocaleTimeString("ru-RU", {hour:"2-digit",minute:"2-digit",second:"2-digit"}) + " · каждые 15 с";
  const query = params();
  query.delete("page"); query.delete("per_page");
  $("export-csv").href = "/api/export?" + query + "&format=csv";
  $("export-txt").href = "/api/export?" + query + "&format=txt";
  history.replaceState(null, "", "/?" + query);
}
async function load() {
  if (controller) controller.abort();
  const ownController = new AbortController();
  controller = ownController;
  try {
    const query = params();
    if (initial.has("suffix")) query.set("suffix", initial.get("suffix"));
    const response = await fetch("/api/names?" + query, {signal:ownController.signal});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Не удалось получить результаты");
    if (page > Math.max(1,Math.ceil(data.total/50))) {page = Math.max(1,Math.ceil(data.total/50)); return load();}
    $("request-error").hidden = true;
    render(data);
  } catch (error) {
    if (error.name === "AbortError") return;
    $("request-error").textContent = error.message + ". Повторим запрос автоматически через 15 секунд.";
    $("request-error").hidden = false;
    $("last-refresh").textContent = "Нет связи · повтор через 15 с";
    $("loading").hidden = true;
    $("worker-badge").textContent = "○ Нет связи с сайтом";
    $("worker-badge").className = "worker-badge offline";
  }
}
function filterChanged() {clearTimeout(debounce); page = 1; load();}
$("filters").addEventListener("submit", (event) => {event.preventDefault();filterChanged();});
$("search").addEventListener("input", () => {clearTimeout(debounce);debounce = setTimeout(filterChanged, 250);});
for (const id of ["status", "suffix", "sort"]) $(id).addEventListener("change", filterChanged);
$("clear-filters").addEventListener("click", () => {$("search").value="";$("status").value="available";$("suffix").value="";$("sort").value="newest";filterChanged();});
$("previous").addEventListener("click", () => {if(page>1){page--;load();}});
$("next").addEventListener("click", () => {if(page*50<total){page++;load();}});
document.addEventListener("visibilitychange", () => {if(!document.hidden) load();});
setInterval(() => {if(!document.hidden) load();}, 15000);
load();
