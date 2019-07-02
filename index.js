const fs = require('fs');
const csv = require('fast-csv');

const stream = fs.createReadStream('./_base.csv');

const estados = [];
estados['AC'] = 'Acre';
estados['AL'] = 'Alagoas';
estados['AP'] = 'Amapá';
estados['AM'] = 'Amazonas';
estados['BA'] = 'Bahia';
estados['CE'] = 'Ceará';
estados['DF'] = 'Distrito Federal';
estados['ES'] = 'Espírito Santo';
estados['GO'] = 'Goiás';
estados['MA'] = 'Maranhão';
estados['MT'] = 'Mato Grosso';
estados['MS'] = 'Mato Grosso do Sul';
estados['MG'] = 'Minas Gerais';
estados['PA'] = 'Pará';
estados['PB'] = 'Paraíba';
estados['PR'] = 'Paraná';
estados['PE'] = 'Pernambuco';
estados['PI'] = 'Piauí';
estados['RJ'] = 'Rio de Janeiro';
estados['RN'] = 'Rio Grande do Norte';
estados['RS'] = 'Rio Grande do Sul';
estados['RO'] = 'Rondônia';
estados['RR'] = 'Roraima';
estados['SC'] = 'Santa Catarina';
estados['SP'] = 'São Paulo';
estados['SE'] = 'Sergipe';
estados['TO'] = 'Tocantins';

const saida = {};

function filtraUF(estado) {
  for (const uf in estados) {
    if (estados[uf] == estado) {
      return uf;
    }
  }

  return 0;
}

//console.log(estados)
csv
  .parseStream(stream, {delimiter: ';'})
  .on('error', error => console.error(error))
  .on('data', row => {
    const estado = row[12];

    const uf = filtraUF(estado);
    //console.log(saida[uf]);
    if(saida[uf] == undefined){
      saida[uf] = [];
    }

    saida[uf].push(row);
  })
  .on('end', rowCount => {
    //console.log(saida);
    const header = saida['0'][0];

    //console.log(header);
    for (const uf in saida) {
      if (uf != '0') {
        const dados = [header, ...saida[uf]];
        const ws = fs.createWriteStream(`./csv/${uf}.csv`);  
        csv  
          .write(dados, { headers: false, delimiter: ';' })
          .pipe(ws);
      }
    }

    console.log(`Parsed ${rowCount} rows`);
  });