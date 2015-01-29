/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.html;

import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;

import org.htmlparser.Node;
import org.htmlparser.Parser;
import org.htmlparser.lexer.Lexer;
import org.htmlparser.lexer.Page;
import org.htmlparser.nodes.TagNode;
import org.htmlparser.nodes.TextNode;
import org.htmlparser.util.DefaultParserFeedback;
import org.htmlparser.util.NodeIterator;
import org.htmlparser.util.ParserException;
import org.htmlparser.util.ParserFeedback;
import org.slf4j.Logger
import org.slf4j.LoggerFactory;

import ai.vital.domain.Edge_hasTagElement;
import ai.vital.domain.TagElement;
import ai.vital.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.TransformationVectorUtils;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.vitalsigns.model.container.Payload;

public class HTMLParser_VS {

	private final static Logger log = LoggerFactory.getLogger(HTMLParser_VS.class);
	
	public static void muteHTML(Payload payload, List<TextBlock> tb, String inputHTML) {
		
		
		for(TextBlock b : tb) {
			
			b.text = "";
			TransformationVectorUtils.setTransformationVector(b, new int[0]);
			
		}
		
		
		//assume the document has just been loaded
        ParserFeedback feedback = new DefaultParserFeedback(0);
		
        Lexer lexer = new Lexer (new Page ( inputHTML ) );
		Parser mParser = new Parser ( lexer );
	    
        mParser.setFeedback (feedback);
        
  		try {
			for (NodeIterator iterator = mParser.elements (); iterator.hasMoreNodes (); ) {
				
				Node nextNode = iterator.nextNode();
					
				processNode(payload, inputHTML, nextNode, tb, "");
					
			}
		} catch (ParserException e) {
			e.printStackTrace();
		}
	}

	private static void processNode(Payload payload, String html, Node node, List<TextBlock> tb,
			String indent) {

//		System.out.println(indent  + node.getText());
		
		TextBlock block = null;
		
		for(TextBlock b : tb) {
			
			int start = b.textBlockOffset;
			int end = b.textBlockOffset + b.textBlockLength;
			
			//check if tag is within the range
			
			if(node.getStartPosition() >= start && node.getEndPosition() <= end ) {
				
				block = b;
				
				break;
				
			}
			
			
		}
		
		if(node instanceof TextNode) {
		
			if(block != null) {
				
				String htmlText = html.substring(node.getStartPosition(), node.getEndPosition());

				//
				HTMLTransformation_VS.appendText(block, htmlText);
				
			}
			
			//escape thetextNode.
			
//			textSpans.add(new TextSpan(((TextNode)node).getText(), node.getStartPosition(), node.getEndPosition()));
		
		//general tag node
		} else if(node instanceof TagNode) {
			
			if(block != null) {
				/*XXX tag elements are ignored!
				
				int mLength = node.getEndPosition() - node.getStartPosition();
				HTMLTransformation_VS.appendMuted(block, mLength);
				
				
				TagNode tagNode = (TagNode) node;
				
//				List tagsList = block.getTagElements(payload);
				
				
				
				TagElement tagEl = new TagElement();
//				tagEl.setURI(block.getURI() + "_tagEl_" + ( tagsList.size() + 1));
				tagEl.setURI(block.getURI() + "_tagEl_" + RandomStringUtils.randomAlphanumeric(16));
				tagEl.startPosition = tagNode.getStartPosition();
				tagEl.endPosition = tagNode.getEndPosition();
				
				tagEl.tagValue = tagNode.getText();
						
				if(tagNode.isEmptyXmlTag()) {
					
					tagEl.standaloneTag = true;

					tagEl.closingTag = false;
					tagEl.openingTag = false;
										
				} else  {
				
					tagEl.standaloneTag = false;
					
					tagEl.closingTag = tagNode.isEndTag();
					tagEl.openingTag = !tagNode.isEndTag();
					
				}
				
				
				payload.map.put(tagEl.URI, tagEl);
				
//				block.getTags().add(tagEl);
				payload.putGraphObjects(EdgeUtils.createEdges(block, Arrays.asList(tagEl), Edge_hasTagElement.class, VitalOntology.Edge_hasTagElementURIBase));
				
				 */
			}
			
			
			/*
			boolean emptyXmlTag = ((TagNode) node).isEmptyXmlTag();
			
			TagNode tagNode = (TagNode) node;
			if( ! tagNode.isEndTag() ) {
				startTags.add(new TagTextSpan(tagNode.getText(), tagNode.getStartPosition(), tagNode.getEndPosition(), tagNode.getTagName()));
				
			} else {
				
				stopTags.add(new TagTextSpan(tagNode.getText(), tagNode.getStartPosition(), tagNode.getEndPosition(), tagNode.getTagName()));
			}
			*/
			
		}
		
		
    	Node firstChild = node.getFirstChild();
    	
		if(firstChild != null) {
			
			processNode(payload, html, firstChild, tb, indent + "\t");
			
			Node nextSibling = firstChild.getNextSibling();
			
			while( nextSibling != null) {
				
				processNode(payload, html, nextSibling, tb, indent + "\t");
				
				nextSibling = nextSibling.getNextSibling();
				
			}
			
		} 
		
		
	}
	
}
