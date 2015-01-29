/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain.rdf.converters;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TagElement;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class TextBlockRDFConverter extends AbstractRDFConverter<TextBlock> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.TextBlock;
	}

	@Override
	public void delete(String uri, Model m) {
		deleteListPropertyAsEdges(m, m.getResource(uri), VitalOntology.Edge_hasSentence, AbstractRDFConverter.converters.get(Sentence.class));
		deleteListPropertyAsEdges(m, m.getResource(uri), VitalOntology.Edge_hasTagElement, AbstractRDFConverter.converters.get(TagElement.class));
		super.delete(uri, m);
	}

	@Override
	public TextBlock fromRDF(Model m, Resource s) {

		TextBlock tb = new TextBlock();
		tb.setUri(s.getURI());
		tb.setLength(getIntegerLiteral(s, VitalOntology.hasTextBlockLength, null));
		tb.setOffset(getIntegerLiteral(s, VitalOntology.hasTextBlockOffset, null));
		tb.setSentences(getListPropertyFromEdges(Sentence.class, m, s, VitalOntology.Edge_hasSentence));
		tb.setTags(getListPropertyFromEdges(TagElement.class, m, s, VitalOntology.Edge_hasTagElement));
		tb.setText(getStringLiteral(s, VitalOntology.hasText));
		List<Integer> vector = new ArrayList<Integer>();
		String vstring = getStringLiteral(s, VitalOntology.hasTransformationVector);
		if(vstring != null) {
			String[] split = vstring.split(",");
			for(String _s : split) {
				if(!_s.isEmpty()) {
					vector.add(Integer.parseInt(_s));
				}
			}
		}
		int[] va = new int[vector.size()];
		for(int i = 0; i < vector.size(); i++) {
			va[i] = vector.get(i);
		}
		
		tb.setTransformationVector(va);
		return tb;
	}

	@Override
	protected void fill(TextBlock tb, Model m, Resource r) {

		addIntegerLiteral(r, VitalOntology.hasTextBlockLength, tb.getLength());
		addIntegerLiteral(r, VitalOntology.hasTextBlockOffset, tb.getOffset());
		
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasSentence, VitalOntology.Edge_hasSentenceURIBase, tb.getSentences());
		
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasTagElement, VitalOntology.Edge_hasTagElementURIBase, tb.getTags());

		addStringLiteral(r, VitalOntology.hasText, tb.getText());
		
		int[] tv = tb.getTransformationVector();
		if(tv != null) {
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < tv.length; i++) {
				if(sb.length() > 0) {
					sb.append(',');
				}
				sb.append(tv[i]);
			}
			addStringLiteral(r, VitalOntology.hasTransformationVector, sb.toString());
		}
	}

}
