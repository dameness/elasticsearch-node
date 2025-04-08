import 'dotenv/config';

import readline from 'readline';
import { Client } from '@elastic/elasticsearch';

const esClient = new Client({ node: process.env.ELASTICSEARCH_NODE_URL });

async function searchProducts(query) {
  try {
    const response = await esClient.search({
      index: 'produtos',
      size: 20,
      query: {
        bool: {
          should: [
            {
              multi_match: {
                query,
                fields: ['cd_produto', 'nm_produto', 'nm_marca'],
                fuzziness: 'AUTO',
              },
            },
            {
              multi_match: {
                query: query + '*',
                fields: ['cd_produto', 'nm_produto', 'nm_marca'],
                type: 'bool_prefix',
              },
            },
          ],
          minimum_should_match: 1,
        },
      },
    });

    const { hits } = response.hits;

    if (hits.length === 0) {
      console.log('Nenhum produto encontrado.');
    } else {
      console.log(`🛒 Resultados para "${query}":\n`);

      hits.forEach((hit, index) => {
        console.log(
          `${index + 1}. ${hit._source.nm_produto} (cd_produto: ${hit._id})`
        );
      });
    }
  } catch (error) {
    console.error('Erro na busca:', error);
  }
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

rl.question('🔍 Digite uma palavra para buscar produtos: ', (wordInput) => {
  searchProducts(wordInput).then(() => rl.close());
});
