import {createServer} from 'node:http';
import {randomUUID,createHash,timingSafeEqual} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {WebSocketServer,WebSocket} from 'ws';

const secureHeaders={
 'Content-Security-Policy':"default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'; form-action 'self'",
 'X-Content-Type-Options':'nosniff','X-Frame-Options':'DENY','Referrer-Policy':'no-referrer',
 'Permissions-Policy':'camera=(), microphone=(), geolocation=(), payment=(), usb=()',
 'Strict-Transport-Security':'max-age=31536000','X-Robots-Tag':'noindex, nofollow, noarchive',
};
const apiPath=/^\/api\/(session|status|evaluate|progress|understanding\/\d+|daily(?:\/(?:prepare|notes|20\d{2}-\d{2}-\d{2}(?:\/evaluate)?))?|discussions\/(?:course\/\d+|daily\/20\d{2}-\d{2}-\d{2}))$/;
export function createGateway({token,origin,assets,timeoutMs=110000}){
 if(!/^[A-Za-z0-9_-]{43}$/.test(token))throw Error('A private bridge credential is required.');
 const canonical=new URL(origin);
 if(canonical.protocol!=='https:'||canonical.origin!==origin)throw Error('Exact HTTPS origin required.');
 const tokenHash=createHash('sha256').update('Bearer '+token).digest();
 const authenticated=value=>typeof value==='string'&&timingSafeEqual(tokenHash,createHash('sha256').update(value).digest());
 let bridge,windowAt=Date.now(),requests=0,closed=false;
 const pending=new Map();
 const reply=(res,status,body,headers={})=>{
  if(res.destroyed||res.writableEnded)return;
  res.writeHead(status,{...secureHeaders,'Cache-Control':'no-store',...headers});res.end(body);
 };
 const error=(res,message,status=503)=>reply(res,status,JSON.stringify({error:message}),{'Content-Type':'application/json; charset=utf-8'});
 const take=id=>{const entry=pending.get(id);if(entry){pending.delete(id);clearTimeout(entry.timer);}return entry;};
 const cancelAll=()=>{for(const id of pending.keys()){const entry=take(id);error(entry.res,'Сервер тренажёра переподключается. Черновик остаётся в браузере.');}};
 const server=createServer(async(req,res)=>{
  try{
   if(req.headers.host!==canonical.host)return error(res,'Недопустимый адрес.',400);
   if(Date.now()-windowAt>=60000){windowAt=Date.now();requests=0;}
   if(++requests>240)return error(res,'Слишком много запросов. Попробуй через минуту.',429);
   const url=new URL(req.url,origin),path=url.pathname;
   if(path==='/health'&&req.method==='GET')return reply(res,200,JSON.stringify({ok:true,backend:bridge?.readyState===WebSocket.OPEN}),{'Content-Type':'application/json'});
   const asset=assets[path];
   if(asset&&['GET','HEAD'].includes(req.method))return reply(res,200,req.method==='HEAD'?null:Buffer.from(asset.body,'base64'),{'Content-Type':asset.type,'Cache-Control':path.startsWith('/assets/')?'public, max-age=31536000, immutable':'no-store'});
   if(!apiPath.test(path))return error(res,'Страница не найдена.',404);
   if(req.headers['sec-fetch-site']==='cross-site'||(req.headers.origin&&req.headers.origin!==origin))return error(res,'Запрос с другого сайта запрещён.',403);
   if(!['GET','HEAD'].includes(req.method)&&req.headers.origin!==origin)return error(res,'Нужен запрос со страницы тренажёра.',403);
   if(!['GET','HEAD','POST','PUT','DELETE','PATCH','OPTIONS'].includes(req.method))return error(res,'Неподдерживаемый запрос.',405);
   if(!bridge||bridge.readyState!==WebSocket.OPEN)return error(res,'Компьютер автора пока не подключён. Попробуй немного позже; черновик остаётся в браузере.');
   if(pending.size>=16)return error(res,'Сервер занят. Попробуй через минуту.');
   const limit=path==='/api/progress'?1000000:16000;
   if(Number(req.headers['content-length']||0)>limit){req.resume();return error(res,'Слишком большой запрос.',413);}
   const parts=[];let length=0;
   for await(const part of req){length+=part.length;if(length>limit)return error(res,'Слишком большой запрос.',413);parts.push(part);}
   if(!bridge||bridge.readyState!==WebSocket.OPEN)return error(res,'Сервер переподключается. Попробуй ещё раз.');
   const headers={};
   for(const name of ['origin','cookie','content-type','sec-fetch-site','x-gsom-profile'])if(typeof req.headers[name]==='string')headers[name]=req.headers[name];
   headers.host=canonical.host;
   const requestId=randomUUID();
   const timer=setTimeout(()=>{const entry=take(requestId);if(entry)error(entry.res,'Ответ сервера задержался. Черновик сохранён; попробуй ещё раз.',504);},timeoutMs);
   pending.set(requestId,{res,timer});res.on('close',()=>take(requestId));
   bridge.send(JSON.stringify({type:'forwardedRequest',requestId,url:url.pathname+url.search,method:req.method,headers,body:Buffer.concat(parts).toString('base64')}));
  }catch{error(res,'Сервис временно недоступен. Попробуй ещё раз.');}
 });
 const sockets=new WebSocketServer({noServer:true,maxPayload:2000000,perMessageDeflate:false});
 server.on('upgrade',(req,socket,head)=>{
  if(closed||req.url!=='/_bridge'||req.headers.host!==canonical.host||req.headers.origin||!authenticated(req.headers.authorization)){
   socket.end('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');return;
  }
  sockets.handleUpgrade(req,socket,head,peer=>sockets.emit('connection',peer));
 });
 sockets.on('connection',peer=>{
  const previous=bridge;bridge=peer;
  if(previous){previous.send(JSON.stringify({type:'clientDisconnect'}));previous.close(4000,'superseded');cancelAll();}
  peer.on('error',()=>{});
  peer.on('close',()=>{if(bridge===peer){bridge=undefined;cancelAll();}});
  peer.on('message',raw=>{
   let m;try{m=JSON.parse(raw.toString());}catch{return peer.close(1008,'invalid message');}
   if(m.type==='initialise')return peer.send(JSON.stringify({type:'hostnameAssigned',hostname:canonical.hostname}));
   if(m.type!=='forwardedResponse'||typeof m.requestId!=='string'||bridge!==peer)return;
   const entry=take(m.requestId);if(!entry)return;
   if(!Number.isInteger(m.statusCode)||m.statusCode<200||m.statusCode>599||typeof m.body!=='string'||m.body.length>1500000||!m.headers||typeof m.headers!=='object')return error(entry.res,'Некорректный ответ сервера.',502);
   const headers={};
   for(const [name,value]of Object.entries(m.headers)){
    if(!['content-type','cache-control','set-cookie','retry-after'].includes(name.toLowerCase()))continue;
    if(typeof value==='string'&&!/[\r\n]/.test(value))headers[name]=value;
    else if(name.toLowerCase()==='set-cookie'&&Array.isArray(value)&&value.every(v=>typeof v==='string'&&!/[\r\n]/.test(v)))headers[name]=value;
   }
   reply(entry.res,m.statusCode,Buffer.from(m.body,'base64'),headers);
  });
 });
 return {server,close:async()=>{closed=true;cancelAll();for(const peer of sockets.clients)peer.terminate();await new Promise(resolve=>server.close(resolve));sockets.close();}};
}
if(process.argv[1]===fileURLToPath(import.meta.url)){
 const app=createGateway({token:process.env.BRIDGE_TOKEN,origin:process.env.PUBLIC_ORIGIN||process.env.RENDER_EXTERNAL_URL,assets:JSON.parse(readFileSync(new URL('./assets.json',import.meta.url),'utf8'))});
 app.server.listen(Number(process.env.PORT||10000),'0.0.0.0',()=>console.log('Case Club Test gateway ready.'));
 process.on('SIGTERM',()=>void app.close());
}
