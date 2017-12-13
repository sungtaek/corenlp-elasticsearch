package org.lima.elasticsearch.index.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;
import org.lima.elasticsearch.tokenizer.CoreNLPTokenizer;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class CoreNLPTokenizerFactory extends AbstractTokenizerFactory {
	private String lang = "en";
	private List<CoreNLPTokenizer.Paraphrase> paraphrase = null;
	private boolean lowercase = false;
	private boolean lemma = false;
	private List<String> exclude = null;

	private StanfordCoreNLP pipeline = null;

	public CoreNLPTokenizerFactory(IndexSettings indexSettings, String name, Settings settings, String pluginPath) {
		super(indexSettings, name, settings);
		
		boolean preload = settings.getAsBoolean("preload", false);
		lang = settings.get("lang", "en");
		String paraphraseFile = settings.get("paraphrase", null);
		lowercase = settings.getAsBoolean("lowercase", false);
		lemma = settings.getAsBoolean("lemma", false);
		String excludeFile = settings.get("exclude", null);

		if(paraphraseFile != null) {
			paraphraseFile = pluginPath + File.separator + CoreNLPTokenizer.name
					+ File.separator + "paraphrase" + File.separator + paraphraseFile;
			loadParaphrase(paraphraseFile);
		}

		if(excludeFile != null) {
			excludeFile = pluginPath + File.separator + CoreNLPTokenizer.name
					+ File.separator + "exclude" + File.separator + excludeFile;
			loadExclude(excludeFile);
		}

		System.out.println("CoreNLPTokenizerFactory: preload("+preload+") lang("+lang+") paraphrase("+paraphraseFile+")"
				+ " lowercase("+lowercase+") lemma("+lemma+") exclude("+excludeFile+")");
		if(preload) {
			loadPipeline();
		}
	}

	@Override
	public Tokenizer create() {
		if(pipeline == null) {
			loadPipeline();
		}
		return new CoreNLPTokenizer(pipeline, paraphrase, lowercase, lemma, exclude);
	}
	
	private void loadParaphrase(String file) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			paraphrase = new ArrayList<CoreNLPTokenizer.Paraphrase>();
			while((line = br.readLine()) != null) {
				String tok[] = line.split("=>");
				if(tok.length == 2) {
					paraphrase.add(new CoreNLPTokenizer.Paraphrase(tok[0].trim(), tok[1].trim()));
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void loadExclude(String file) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			exclude = new ArrayList<String>();
			while((line = br.readLine()) != null) {
				exclude.add(line);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void loadPipeline() {
		Properties prop = new Properties();
		
		// set annotators pipeline
		StringBuilder annotators = new StringBuilder();
		annotators.append("tokenize");
		annotators.append(",ssplit");
		annotators.append(",pos");
		if(lemma) {
			annotators.append(",lemma");
		}
		prop.setProperty("annotators", annotators.toString());
		
		// set tokenizer
		prop.setProperty("tokenize.language", lang);
		
		// set pos-tagger
		if(lang.equals("en")) {
			if(lowercase) {
				prop.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/english-caseless-left3words-distsim.tagger");
			}
			else {
				prop.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
			}
		}
		
		// create pipeline
		AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
			this.pipeline = new StanfordCoreNLP(prop);
			return null;
		});

	}
}
