package org.lima.elasticsearch.tokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class CoreNLPTokenizer extends Tokenizer {
	public static String name = "corenlp_tokenizer";
	
	public static class Paraphrase {
		public String from;
		public String to;
		public Paraphrase(String from, String to) {
			this.from = from;
			this.to = to;
		}
	}

	private StanfordCoreNLP pipeline;
	private List<Paraphrase> paraphrase;
	private boolean lowercase;
	private boolean lemma;
	private List<String> exclude;
	
	private ArrayList<CoreLabel> tokens;
	private int idx;
	
	private CharTermAttribute charTermAttribute;
	private TypeAttribute typeAttribute;
	private OffsetAttribute offsetAttribute;
	
	public CoreNLPTokenizer(StanfordCoreNLP pipeline, List<Paraphrase> paraphrase, boolean lowercase,
			boolean lemma, List<String> exclude) {
		this.pipeline = pipeline;
		this.paraphrase = paraphrase;
		this.lowercase = lowercase;
		this.lemma = lemma;
		this.exclude = exclude;

		tokens = null;
		idx = 0;

		this.charTermAttribute = addAttribute(CharTermAttribute.class);
		this.typeAttribute = addAttribute(TypeAttribute.class);
		this.offsetAttribute = addAttribute(OffsetAttribute.class);
	}

	@Override
	public boolean incrementToken() throws IOException {
		clearAttributes();
		
		// read document & tagging
		if(tokens == null) {
			String strDoc = getDocument();
			if(paraphrase != null && paraphrase.size() > 0) {
				for(Paraphrase p: paraphrase) {
					strDoc = strDoc.replaceAll("(?i)(\\s|^)"+p.from+"(\\s|$)", " "+p.to+" ");
				}
			}
			if(lowercase) {
				strDoc = strDoc.toLowerCase();
			}
			Annotation doc = new Annotation(strDoc);
			pipeline.annotate(doc);
			List<CoreMap> sentences = doc.get(SentencesAnnotation.class);
			if(sentences != null && sentences.size() > 0) {
				tokens = new ArrayList<CoreLabel>();
				for(CoreMap sentence: sentences) {
					tokens.addAll(sentence.get(TokensAnnotation.class));
				}
			}
			if(exclude != null && exclude.size() > 0) {
				for(Iterator<CoreLabel> it = tokens.iterator(); it.hasNext();) {
					boolean found = false;
					CoreLabel token = it.next();
					String term;
					if(lemma) {
						term = token.get(LemmaAnnotation.class);
					}
					else {
						term = token.get(TextAnnotation.class);
					}
					term = term + "/" + token.get(PartOfSpeechAnnotation.class);
					for(String format: exclude) {
						if(Pattern.matches(format, term)) {
							found = true;
							break;
						}
					}
					if(found) {
						it.remove();
					}
				}
			}
			idx = 0;
		}
		
		// set token/tag
		if(tokens != null && tokens.size() > idx) {
			CoreLabel token = tokens.get(idx);
			if(lemma) {
				charTermAttribute.append(token.get(LemmaAnnotation.class));
			}
			else {
				charTermAttribute.append(token.get(TextAnnotation.class));
			}
			typeAttribute.setType(token.get(PartOfSpeechAnnotation.class));
			offsetAttribute.setOffset(token.get(CharacterOffsetBeginAnnotation.class),
					token.get(CharacterOffsetEndAnnotation.class));
			idx++;
			return true;
		}
		
		initValues();
		return false;
	}
	
	@Override
	public final void reset() throws IOException {
		super.reset();
		initValues();
	}
	
	private void initValues() {
		tokens = null;
		idx = 0;
	}

	private String getDocument() throws IOException {
		StringBuilder doc = new StringBuilder();
		char[] tmp = new char[1024];
		int len;
		while((len = input.read(tmp)) != -1) {
			doc.append(new String(tmp, 0, len));
		}
		return doc.toString();
	}
}
