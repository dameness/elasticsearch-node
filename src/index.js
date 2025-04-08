import 'dotenv/config';

import pg from 'pg';
import { Client } from '@elastic/elasticsearch';

const esClient = new Client({ node: process.env.ELASTICSEARCH_NODE_URL });

const db = new pg.Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_DATABASE,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

async function indexProducts() {
  try {
    const productsQuery = `
      SELECT p.cd_produto, p.nm_produto, m.nm_marca,
             COALESCE((SELECT ds_imagem
                         FROM produto_imagem pim
                        WHERE pim.cd_produto = p.cd_produto
                        LIMIT 1),
                      (SELECT 'https://veplex-imagens.s3.sa-east-1.amazonaws.com/produto/' || ds_codigo || '/' || ds_codigo || '.jpg'
                         FROM produto_unidade
                        WHERE cd_produto = p.cd_produto
                        LIMIT 1)) AS ds_imagem_produto
        FROM produto p
        JOIN fabricante f ON f.cd_fabricante = p.cd_fabricante
        JOIN marca m ON m.cd_marca = f.cd_marca
    `;

    const result = await db.query(productsQuery);

    const bulkBody = [];
    const indexName = 'produtos';
    const batchSize = 1000;

    let i = 0;

    for (const row of result.rows) {
      bulkBody.push({ index: { _index: indexName, _id: row.cd_produto } });

      bulkBody.push({
        cd_produto: row.cd_produto,
        nm_produto: row.nm_produto,
        nm_marca: row.nm_marca,
        ds_imagem_produto: row.ds_imagem_produto,
      });

      i++;

      if (i % batchSize === 0) {
        await sendBatch(bulkBody);
        console.log(`✅ Lote de ${batchSize} produtos indexado!`);
        bulkBody.length = 0;
      }
    }

    if (bulkBody.length > 0) {
      await sendBatch(bulkBody);
      console.log('✅ Lote final indexado!');
    }

    console.log('🏁 Indexação completa!');
    process.exit(0);
  } catch (error) {
    console.error('Erro durante a indexação:', error);
    process.exit(1);
  }
}

async function sendBatch(body) {
  const response = await esClient.bulk({ body });

  if (response.errors) {
    console.error('⚠️  Erro ao indexar lote!');
  }
}

indexProducts();
