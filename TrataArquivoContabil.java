package trataCampo;
import java.util.regex.*;
import java.util.*;
import java.io.*;

public class TrataArquivoContabil{

	private String nomeArquivoIn;
	private String nomeArquivoOut;
	private String tipoDelim;
	private String coding;
	private int posicao;

	private TrataArquivoContabil(String nomeIn, String delim, String nomeOut, String coding){
		this.nomeArquivoIn = nomeIn;
		this.nomeArquivoOut = nomeOut;
		this.tipoDelim = delim;
		this.coding = coding;
		TrataArquivoContabil.leArquivo(nomeArquivoIn, tipoDelim, nomeArquivoOut, coding);
	}

	public void setPosicao(int i){
		posicao = i;
	}

	private TrataArquivoContabil(){}
	
	public static void main (String args[]){	
	
		try{	
			TrataArquivoContabil.defineArquivo("./ArqEntrada","./ArqSaida", "txt;tmp");
		}catch(IOException ioe){
			System.err.println("Arquivo não encontrado");
		}	
	}
	
	public static void defineArquivo(String dirEnt, String dirSaida, String ext) throws IOException{

		final String[] suffixArq = ext.split(";");
		FilenameFilter fileNameFilter = new FilenameFilter() {
        	    @Override
	            public boolean accept(File dir, String name) {
			for(String a : suffixArq){
				if(name.lastIndexOf('.')>0){
        		          int lastIndex = name.lastIndexOf('.')+1;
	        	          String str = name.substring(lastIndex);
                		  if(str.equals(a)){
	        	             return true;
		                  }
        		       }
        		    }
			return false;
			}
	         };
		File listArq = new File(dirEnt);
		File arqSaida = new File(dirSaida);
		arqSaida.mkdirs();
		File[] arquivos = listArq.listFiles(fileNameFilter);
		for(File s : arquivos){
			String nomeArqEnt = s.toString().substring(s.toString().indexOf("/")+1);
			String nomeArqSaida = String.format("%s/%s.csv",dirSaida,s.toString().substring(s.toString().lastIndexOf("/")+1,s.toString().lastIndexOf(".")));
			System.out.println("Inicio do tratamento do arquivo "+nomeArqEnt);
			TrataArquivoContabil.leArquivo(nomeArqEnt, "\\n",nomeArqSaida, "utf-8");
			System.out.println("Termino do tratamento do arquivo "+nomeArqEnt);
		}
	}	

	public static void leArquivo(String fileIn, String delimiter, String fileOut, String coding){

		String dados = null;
		String registro = null;
		BufferedWriter bufferedWriter = null;
		TrataArquivoContabil novo = new TrataArquivoContabil();
		
		try(Scanner scanner = new Scanner(new FileReader(fileIn)).useDelimiter(delimiter);){
			try{
				bufferedWriter = new BufferedWriter(new FileWriter(fileOut));
				String noConta, tpConta, pf, reg, hist, dadosAux;
        	                noConta = tpConta = pf = reg = hist = dadosAux = null;
				bufferedWriter.write("Conta|Tipo de conta|PF|Dt Lanc|Un. Gest.|Gestão|Evento|Documento|Vlr. Deb|Vlr. Cred.|Vlr. Saldo|Dc|Historico\n");	
				while(scanner.hasNext()) {
				
					String linha = scanner.next();
				
					dados = dadosAux;
	
					if(linha.startsWith("CONTA")){
						if(novo.getNoConta(linha) != null){
							noConta = novo.getNoConta(linha);
							tpConta = novo.getTpConta(linha);
						}
					}

					if(linha.startsWith("C/C")){
						if(novo.getPF(linha) != null){
                                        	        pf = novo.getPF(linha);
                                	        }
					}

					if(TrataArquivoContabil.startsWith(Pattern.compile("\\d{2}\\w{3}\\d{4}\\s+"), linha)){
						reg = String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s",novo.getDtTrans(linha),novo.getUG(linha),novo.getGestao(linha),novo.getEvento(linha),novo.getDocumento(linha),novo.getVlrDeb(linha),novo.getVlrCred(linha),novo.getVlrSald(linha),novo.getDC(linha));	
						novo.setPosicao(0);
					}

					if(linha.startsWith("HISTORICO")){
						hist = String.format("%s",novo.getHist(linha, "ini"));
					}
	
					if(TrataArquivoContabil.startsWith(Pattern.compile("\\s{4,14}\\S+"), linha)){
						hist = String.format("%s%s",hist,novo.getHist(linha, "cont"));
					}

					if(noConta != null && tpConta != null && pf != null && reg != null && hist != null){
						dadosAux = String.format("%s|%s|%s|%s|%s\n",noConta,tpConta,pf,reg,hist);
						if(dados != null && dados.equals(dadosAux)){
	                                                bufferedWriter.write(dados);
							hist = null;
        	                                }
					}
				}
                	}catch(IOException e){
        	              System.out.printf("Arquivo já existe de saida. ");
	                }finally{
                	      bufferedWriter.flush();
        	              bufferedWriter.close();
	                }
		}catch (IOException | NullPointerException | ArrayIndexOutOfBoundsException e){
			e.printStackTrace();
		}
	}

	public String getNoConta(String linha){
		String conta;
		conta = linha.substring(linha.indexOf(":")+1, linha.indexOf("=")).trim();
	   return conta;
	}

	public String getPF(String linha){
                String pf;
		int ini = linha.indexOf(":")+2;
		int fim = TrataArquivoContabil.indexOf(Pattern.compile("\\s{5,}"),linha);
                pf = linha.substring(ini,fim);
           return pf;
	}

        public String getTpConta(String linha){
                String conta;
        	conta = linha.substring(linha.indexOf("=")+2).trim();
            return conta;
	}

	public String getDtTrans(String linha){
		String dtTrans;
		if(linha.substring(posicao,9).trim().isEmpty()){
			dtTrans=null;
			posicao=9;
		}else{
		int ini = TrataArquivoContabil.indexOf(Pattern.compile("\\d{2}\\w{3}\\d{4}\\s+"), linha, posicao);
		int fim = TrataArquivoContabil.indexOf(Pattern.compile("\\s{2,}"), linha, posicao);
		dtTrans = linha.substring(ini, fim);
		posicao = fim;
		}
	   return dtTrans;		
	}

	public String getUG(String linha){
		String ug;
		if(linha.substring(posicao,17).trim().isEmpty()){
                        ug=null;
                        posicao=17;
                }else{
		int ini = TrataArquivoContabil.indexOf(Pattern.compile("\\s+\\d{6}"), linha, posicao)+2;
		int fim = ini+6;
		ug = linha.substring(ini,fim);
		posicao = fim;
		}
	   return ug;
	}

	public String getGestao(String linha){
		String gest;
		if(linha.substring(posicao,23).trim().isEmpty()){
                        gest=null;
                        posicao=23;
                }else{
		int ini = TrataArquivoContabil.indexOf(Pattern.compile("\\s+\\d{5}\\s+"), linha, posicao)+1;
                int fim = ini+5;
                gest = linha.substring(ini,fim);
		posicao = fim;
		}
	   return gest;
	}

	public String getEvento(String linha){
                String event;
		if(linha.substring(posicao,32).trim().isEmpty()){
                        event=null;
                        posicao=32;
                }else{
		int ini = TrataArquivoContabil.indexOf(Pattern.compile("\\s{3}\\d{6}\\s{3}"), linha, posicao)+3;
                int fim = ini+6;
                event = linha.substring(ini,fim);
		posicao = fim;
		}
           return event;
	}

	public String getDocumento(String linha){
                String doc;
		if(linha.substring(posicao,46).trim().isEmpty()){
                        doc=null;
                        posicao=46;
                }else{
		int ini = TrataArquivoContabil.indexOf(Pattern.compile("\\s{3}\\d{4}\\w{2}\\d{5}\\s+"), linha, posicao)+3;
                int fim = ini+11;
	        doc = linha.substring(ini,fim);
		posicao = fim;
		}
           return doc;
	}

	public String getVlrDeb(String linha){
                String vlrDeb;
		if(linha.substring(posicao,127).trim().isEmpty()){
                        vlrDeb="0,0";
                        posicao=127;
                }else{
                int ini = TrataArquivoContabil.indexOf(Pattern.compile("(\\d{1,3}.)|(\\d{1,3}.\\d{1,3}),\\d{1,2}\\s+"), linha, posicao);
                int fim = TrataArquivoContabil.indexOf(Pattern.compile(",\\d+\\s+"), linha, posicao)+3;
                vlrDeb = linha.substring(ini,fim);
		posicao = fim;
		}
           return vlrDeb;
	}

	public String getVlrCred(String linha){
                String vlrCred;
		if(linha.substring(posicao,145).trim().isEmpty()){
			vlrCred="0,0";
			posicao=145;
		}else{
		int ini = TrataArquivoContabil.indexOf(Pattern.compile("(\\d{1,3}.)|(\\d{1,3}.\\d{1,3}),\\d{1,2}\\s+"), linha, posicao);
                int fim = TrataArquivoContabil.indexOf(Pattern.compile(",\\d+\\s+"), linha, posicao)+3;
                vlrCred = linha.substring(ini,fim);
		posicao = fim;
		}
           return vlrCred;
	}

	public String getVlrSald(String linha){
                String vlrSald;
		if(linha.substring(posicao,165).trim().isEmpty()){
                        vlrSald="0,0";
                        posicao=165;
                }else{
		int ini = TrataArquivoContabil.indexOf(Pattern.compile("(\\d{1,3}.)|(\\d{1,3}.\\d{1,3}),\\d{1,2}\\s+"), linha, posicao);
                int fim = TrataArquivoContabil.indexOf(Pattern.compile(",\\d+\\s+"), linha, posicao)+3;
                vlrSald = linha.substring(ini,fim);
                posicao = fim;
		}
           return vlrSald;
	}

	public String getDC(String linha) {
                String dc;
		if(linha.substring(posicao,170).trim().isEmpty()){
                        dc=null;
                        posicao=170;
                }else{
		int ini = TrataArquivoContabil.indexOf(Pattern.compile("\\w{1}"), linha, posicao);
		int fim = ini+2;
                dc = linha.substring(ini,fim).trim();
                posicao = fim;
		}
           return dc;
	}

	public String getHist(String linha, String tipo){
		String hist = null;
		if(tipo == "ini"){
			hist = linha.substring(linha.indexOf(":")+1).trim();
		}
		if(tipo == "cont"){
			hist = linha.trim();
		}
	  return hist;
	}

	public static int indexOf(Pattern pattern, String s, int i) {
                   Matcher matcher = pattern.matcher(s);
            return matcher.find(i) ? matcher.start() : -1;
        }

	public static int indexOf(Pattern pattern, String s) {
		   Matcher matcher = pattern.matcher(s);
	    return matcher.find() ? matcher.start() : -1;
	}

	public static boolean startsWith(Pattern pattern, String linha){
		Matcher matcher = pattern.matcher(linha);
		if(matcher.find() && matcher.start() == 0){
			return true;
		}else{
			return false;
		}
	}
}
