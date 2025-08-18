Objetivo do teste:

Explorar as habilidades e o conhecimento sobre: desenvolvimento de scripts, tratamento de dados, estrutura de dados, ordenação, parse e formatação.

O formato do access log é definido por:

"%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""

%h – Remote host (client IP address)
%l – User identity, or dash, if none (often not used)
%u – Username, via HTTP authentication, or dash if not used
%t – Timestamp of when Apache received the HTTP request
\”%r\ – The actual request itself from the client
%>s – The status code Apache returns in response to the request
%T – The amout of time to response in milliseconds.
\”%{Referer}i\” – Referrer header, or dash if not used  (In other words, did they click a URL on another site to come to your site)
\”%{User-agent}i\ – User agent (contains information about the requester’s browser/OS/etc)

- formatar uma saída do log em json contendo a lista de request apresentada no log, cada objeto dentro da lista deve conter as propriedades de uma entrada no log como remote_host, date, request, status_code, response_time, reffer, user_agent.
- encontrar os 10 maiores tempos de resposta com sucesso do servidor na chamada GET /maunal/ com a origem do tráfego (reffer) igual a "http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1"
- formatar uma saída em arquivo físico do access.log com a data em formato UNIX timestamp %Y-%m-%d %H:%M:%S e o IP convertido em um hash MD5
- formatar uma saída em arquivo físico agrupando a soma total de requests por dia do ano
- formatar uma saída em arquivo físico com endereços de IP únicos, um IP por linha, contidos no log com a última data de request realizado pelo remote IP

Apresentar na solução:

- código desenvolvido para a resolução do teste
- instruções de execução da solução enviada
- os arquivos gerados pela execução do código
