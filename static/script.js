let rawData = []; // Variável para armazenar os dados carregados

// Inicializa a DataTable com os dados
function initializeTable(data) {
    if ($.fn.DataTable.isDataTable('#data-table')) {
        $('#data-table').DataTable().destroy();
        $('#data-table').empty();
    }

    if (!data || data.length === 0) {
        console.error("No data received");
        return;
    }

    $('#data-table').DataTable({
        data: data,
        columns: Object.keys(data[0]).map(key => ({ title: key, data: key })),
        paging: true,
        searching: true,
        ordering: true,
        autoWidth: true,
        // scrollX: true,
        // scrollY: '90vh',
        // scrollCollapse: true,
        // fixedHeader: true
    });
}

async function uploadFile() {
    const fileInput = document.getElementById('fileInput');
    const file = fileInput.files[0];

    if (!file) {
        alert("Por favor, selecione um arquivo primeiro.");
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    try {
        const response = await fetch('http://127.0.0.1:5000/upload', {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            throw new Error("Erro no upload do arquivo.");
        }

        const rawData = await response.json();

        // Verifica se os dados foram retornados
        if (rawData && rawData.data) {
            initializeTable(rawData.data);  // Chama a função para exibir os dados
        } else {
            alert("Nenhum dado foi retornado.");
        }
    } catch (error) {
        console.error("Erro:", error);
        alert("Falha ao ler o arquivo.");
    }
}

async function transpor() {
    try {
        const response = await fetch('http://127.0.0.1:5000/transpor', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ data: rawData })

        });

        if (!response.ok) {
            const errorResult = await response.json();
            alert("Erro: " + errorResult.error);
            return;
        }

        const result = await response.json();
        rawData = result;
        initializeTable(rawData);
    } catch (error) {
        console.error("Erro:", error);
        alert("Falha ao carregar o arquivo.");
    }
}

// Função para limpar dados do arquivo .txt
async function cleanData() {
    if (!rawData || rawData.length === 0) {
        alert("Por favor, carregue um arquivo primeiro.");
        return;
    }

    try {
        const response = await fetch('/clean_data', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ data: rawData })
        });

        if (!response.ok) {
            const errorResult = await response.json();
            alert("Erro: " + errorResult.error);
            return;
        }

        const result = await response.json();
        rawData = result; // Atualiza o rawData com os dados limpos
        initializeTable(rawData); // Exibe a tabela com os dados limpos
    } catch (error) {
        console.error("Erro:", error);
        alert("Falha ao limpar os dados.");
    }
}

// Função para aplicar fórmula personalizada
async function calcularNovaColuna() {
    const formula = document.getElementById('formulaInput').value;
    const newColumnName = document.getElementById('newColumnName').value;
    console.log("Nova coluna:", newColumnName);
    console.log("Fórmula:", formula);

    if (!formula) {
        alert("Por favor, insira uma fórmula.");
        return;
    }

    const regex = /^[\w\d\s()+\-*/x.]+$/; // Permitir colunas, números, operadores matemáticos e espaços
    if (!regex.test(formula)) {
        alert("Fórmula inválida. Use apenas operadores matemáticos (+, -, *, /), parênteses, números e nomes de colunas.");
        return;
    }

    try {
        // Enviar dados para o backend
        const response = await fetch('http://127.0.0.1:5000/calcular_nova_coluna', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                formula: formula,
                new_column: newColumnName,
                data: rawData // Substitua por seus dados reais do DataFrame
            })
        });    
    
        if (response.ok) {
            const result = await response.json();
            rawData = result;
            initializeTable(rawData);
            closeFormulaModal(); // Função para atualizar a tabela no frontend
        } else {
            const error = await response.json();
            alert(`Erro: ${error.error}`);
        }
    } catch (error) {
        console.error("Erro ao aplicar a fórmula:", error);
        alert("Erro ao aplicar a fórmula. Verifique o console para mais detalhes.");
    }
}       

async function submitRenameColumn() {
    const currentColumn = document.getElementById('colunaAtual').value;
    const newColumnName = document.getElementById('novaColuna').value;

    console.log("Nome da coluna atual:", currentColumn);
    console.log("Novo nome da coluna:", newColumnName);

    if (!currentColumn || !newColumnName) {
        alert("Por favor, preencha ambos os campos.");
        return;
    }

    try {
        const response = await fetch('http://127.0.0.1:5000/rename_column', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ currentColumn, newColumnName, rawData })
        });

        if (!response.ok) {
            const errorResult = await response.json();
            alert("Erro: " + errorResult.error);
            return;
        }

        const result = await response.json();
        alert("Coluna renomeada com sucesso!");
        rawData = result;
        initializeTable(rawData); // Atualiza a tabela com os novos nomes de coluna
        closeModal();
    } catch (error) {
        console.error("Erro:", error);
        alert("Falha ao renomear a coluna.");
    }
}
async function trocarValor() {
    const columnName = document.getElementById('columnName').value;
    const currentValue = document.getElementById('currentValue').value;
    const newValue = document.getElementById('newValue').value;

    if (!columnName || !currentValue || !newValue) {
        alert("Por favor, preencha todos os campos.");
        return;
    }

    // Enviar os dados para o backend (exemplo de integração)
    const requestData = {
        column: columnName,
        oldValue: currentValue,
        newValue: newValue,
        data: rawData
    };

    try {
        const response = await fetch('http://127.0.0.1:5000/replace_value', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        });

        if (!response.ok) {
            const errorResult = await response.json();
            alert("Erro: " + errorResult.error);
            return;
        }

        const result = await response.json();
        alert("Valores Substituídos com sucesso!");
        rawData = result;
        initializeTable(rawData); // Atualiza a tabela com os novos 
        closeModal();
    } catch (error) {
        console.error("Erro:", error);
        alert("Falha ao renomear a coluna.");
    }

}

function carregarBancoDeDados() {
    const dbType = document.getElementById('dbType').value;
    const dbTable = document.getElementById('dbTable').value;

    if (!dbType) {
        alert("Por favor, selecione o tipo de banco de dados.");
        return;
    }

    if (!dbTable) {
        alert("Por favor, insira o nome da tabela.");
        return;
    }

    fetch('/database', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            action: 'set_database', // Primeiro configura o banco
            db_type: dbType
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            alert(`Erro ao configurar o banco: ${data.error}`);
            return;
        }

        // Configuração bem-sucedida, agora carrega os dados da tabela
        fetch('/database', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                action: 'load_table',
                table_name: dbTable
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                alert(`Erro ao carregar dados: ${data.error}`);
            } else {
                alert(`Dados carregados com sucesso: ${data.row_count} registros encontrados.`);
                rawData = data.data; // Atualiza os dados carregados
                initializeTable(rawData); // Atualiza a tabela
                console.log("Dados carregados:", rawData);
                closeDatabaseModal();
            }
        })
        .catch(error => {
            console.error('Erro ao carregar dados:', error);
            alert('Erro ao carregar dados.');
        });
    })
    .catch(error => {
        console.error('Erro ao configurar banco:', error);
        alert('Erro ao configurar o banco de dados.');
    });
}

async function sumarizar() {
    try {
        // Obter valores dos campos de entrada
        const colunaAgrupamento = document.getElementById('agrupamentoInput').value;
        const colunaOperacoes = document.getElementById('colunaOperacoesInput').value;
        const operacoesStr = document.getElementById('operacoesInput').value;

        // Validar campos de entrada
        if (!colunaAgrupamento || !colunaOperacoes || !operacoesStr) {
            throw new Error('Todos os campos são obrigatórios.');
        }

        // Processar operações
        const operacoes = operacoesStr.split(',').map(op => op.trim());

        // Estruturar dados para a requisição
        const data = {
            agrupamento: [colunaAgrupamento],
            operacoes: {
                [colunaOperacoes]: operacoes
            }
        };

        // Enviar a requisição POST
        const response = await fetch('/sumarizar', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });

        // Verificar se a requisição foi bem-sucedida
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(`Erro: ${errorData.error}`);
        }

        // Obter os dados da resposta
        const resultado = await response.json();
        console.log('Resultado das operações:', resultado);
        initializeTable(resultado);

    } catch (error) {
        console.error('Erro ao aplicar operações:', error);

        // Exibir erro no frontend
        alert(`Erro ao aplicar operações: ${error.message}`);
    }
}

async function calcularMediaPonderada() {
    const valueColumn = document.getElementById('valueColumn').value;
    const weightColumn = document.getElementById('weightColumn').value;
    const outputColumn = document.getElementById('outputColumn').value || "Media_Ponderada"; // Nome padrão

    console.log("Coluna de Valores:", valueColumn);
    console.log("Coluna de Pesos:", weightColumn);
    console.log("Coluna de Saída:", outputColumn);

    // Validação
    if (!valueColumn || !weightColumn) {
        alert("Por favor, insira os nomes das colunas de valores e de pesos.");
        return;
    }

    try {
        // Enviar dados para o backend
        const response = await fetch('http://127.0.0.1:5000/calcular_media_ponderada', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                valor_col: valueColumn,
                peso_col: weightColumn,
                output_col: outputColumn
            })
        });

        if (response.ok) {
            const result = await response.json();
            rawData = result; // Atualiza os dados com a nova coluna
            initializeTable(rawData); // Atualiza a tabela no frontend
            closeWeightedAverageModal(); // Fecha o modal
        } else {
            const error = await response.json();
            alert(`Erro: ${error.error}`);
        }
    } catch (error) {
        console.error("Erro ao calcular a média ponderada:", error);
        alert("Erro ao calcular a média ponderada. Verifique o console para mais detalhes.");
    }
}

function closeWeightedAverageModal() {
    document.getElementById('weightedAverageModal').style.display = 'none';
}




    

